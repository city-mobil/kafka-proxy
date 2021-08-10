use std::cell::Cell;
use std::ops::Deref;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Rule {
    pub topic_name: String,
    pub max_requests_per_minute: u32,
}

#[derive(Debug)]
struct Bucket {
    last_ts: Cell<i32>,
    count: Cell<i32>,
}

impl Bucket {
    pub fn get_count(&self) -> i32 {
        self.count.get()
    }

    pub fn get_last_ts(&self) -> i32 {
        self.last_ts.get()
    }

    pub fn set_count(&self, count: i32) {
        self.count.set(count);
    }
}

struct BucketStorage {
    buckets: std::vec::Vec<Bucket>,
}

impl BucketStorage {
    pub fn new() -> BucketStorage {
        let mut v = std::vec::Vec::new();
        for _ in 0..BUCKET_COUNT {
            v.push(Bucket {
                count: Cell::new(0),
                last_ts: Cell::new(0),
            });
        }
        BucketStorage { buckets: v }
    }
}

type Storage = std::collections::HashMap<String, std::sync::Mutex<BucketStorage>>;

pub struct Limiter {
    container: std::sync::Mutex<Storage>,
    rules: std::collections::HashMap<String, Rule>,
}

const BUCKET_COUNT: u64 = 60;

impl Limiter {
    pub fn check(&mut self, topic_name: &String) -> Result<bool, String> {
        if topic_name == "" {
            // NOTE(shmel1k): if topic_name is empty, ratelimit is not checked.
            return Ok(true);
        }

        let rule = self.rules.get(topic_name);
        let max_attempts = match rule {
            Some(t) => t.max_requests_per_minute,
            None => return Ok(true),
        };

        if max_attempts == 0 {
            return Ok(true);
        }

        let lock = self.container.lock();
        let storage = match lock {
            Err(e) => return Err(e.to_string()),
            Ok(l) => l,
        };

        let mut bucket_storage = storage.get(topic_name).unwrap();
        let bucket_lock = bucket_storage.lock();
        let mut bucket = match bucket_lock {
            Err(e) => return Err(e.to_string()),
            Ok(l) => l,
        };
        let ts = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(err) => return Err(err.to_string()),
        };
        let bucket_slot = Limiter::calculate_bucket(ts);
        if let Some(b) = &bucket.buckets.get_mut(1) {
            //
        }
        println!(
            "{}",
            bucket
                .buckets
                .get(bucket_slot as usize)
                .unwrap()
                .get_count()
        );
        Ok(false)
    }

    fn calculate_bucket(ts: u64) -> u64 {
        return ts % BUCKET_COUNT;
    }

    pub fn new(rules: std::vec::Vec<Rule>) -> Limiter {
        let mut rls = std::collections::HashMap::new();
        let mut storage = Storage::new();
        for rule in rules {
            rls.insert(rule.topic_name.clone(), rule.clone());
            let b = BucketStorage::new();
            storage.insert(rule.topic_name, std::sync::Mutex::new(b));
        }

        Limiter {
            rules: rls,
            container: std::sync::Mutex::from(storage),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Bucket, BucketStorage, Limiter, Rule, BUCKET_COUNT};
    use std::cell::Cell;

    fn eq_buckets(a: &Bucket, b: &Bucket) -> bool {
        return a.count == b.count && a.last_ts == b.last_ts;
    }

    fn vec_compare(va: &[Bucket], vb: &[Bucket]) -> bool {
        (va.len() == vb.len()) && va.iter().zip(vb).all(|(a, b)| eq_buckets(a, b))
    }

    #[test]
    fn test_new_bucket_storage() {
        let b = BucketStorage::new();
        let mut v = std::vec::Vec::new();
        for _ in 0..BUCKET_COUNT {
            v.push(Bucket {
                count: Cell::new(0),
                last_ts: Cell::new(0),
            });
        }

        assert_eq!(vec_compare(&b.buckets, &v), true);
        assert_eq!(b.buckets.len(), 60);
    }

    #[test]
    fn test_check_ratelimit_empty_topic_name() {
        let mut limiter = Limiter::new(vec![Rule {
            max_requests_per_minute: 0,
            topic_name: String::from("a"),
        }]);
        assert_eq!(limiter.check(&String::from("")).unwrap(), true);
    }

    #[test]
    fn test_check_ratelimit_zero_max_attempts() {
        let mut limiter = Limiter::new(vec![Rule {
            max_requests_per_minute: 0,
            topic_name: String::from("a"),
        }]);
        assert_eq!(limiter.check(&String::from("a")).unwrap(), true);
    }

    #[test]
    fn test_check_ratelimit_non_existing_rule() {
        let mut rules = vec![Rule {
            topic_name: String::from("some_topic_2"),
            max_requests_per_minute: 42,
        }];
        let mut limiter = Limiter::new(rules);
        assert_eq!(limiter.check(&String::from("some_topic")).unwrap(), true);
    }
}
