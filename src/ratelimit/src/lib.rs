use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Rule {
    pub topic_name: String,
    pub max_requests_per_minute: u32,
}

#[derive(Debug)]
struct Bucket {
    last_ts: Cell<u64>,
    count: Cell<u64>,
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
    fn update_bucket(b: &Bucket, ts: u64) {
        let last_ts = b.last_ts.get();
        let count = b.count.get();
        if last_ts != ts {
            b.last_ts.set(ts);
            b.count.set(1);
        } else {
            b.count.set(count + 1);
        }
    }

    fn check_locked(buckets: &mut std::vec::Vec<Bucket>, ts: u64, max_allowed: u32) -> bool {
        let bucket_slot = Limiter::calculate_bucket(ts) as usize;
        if let Some(b) = buckets.get_mut(bucket_slot) {
            Limiter::update_bucket(b, ts);
        }
        let mut count: u64 = 0;
        for v in buckets.iter() {
            let last_ts = v.last_ts.get();
            if last_ts + BUCKET_COUNT >= ts {
                count += v.count.get();
            }
        }
        return count <= max_allowed as u64;
    }

    pub fn check(&self, topic_name: &String) -> Result<bool, String> {
        if topic_name == "" {
            // NOTE(shmel1k): if topic_name is empty, ratelimit is not checked.
            return Ok(true);
        }

        let rule = self.rules.get(topic_name);
        let max_attempts_count = match rule {
            Some(t) => t.max_requests_per_minute,
            None => return Ok(true),
        };

        if max_attempts_count == 0 {
            return Ok(true);
        }

        let lock = self.container.lock();
        let storage = match lock {
            Err(e) => return Err(e.to_string()),
            Ok(l) => l,
        };

        let bucket_storage = storage.get(topic_name).unwrap();
        let bucket_lock = bucket_storage.lock();
        let mut bucket = match bucket_lock {
            Err(e) => return Err(e.to_string()),
            Ok(l) => l,
        };
        let ts = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(err) => return Err(err.to_string()),
        };

        return Ok(Limiter::check_locked(
            &mut bucket.buckets,
            ts,
            max_attempts_count,
        ));
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
    use tokio::test;

    fn eq_buckets(a: &Bucket, b: &Bucket) -> bool {
        return a.count == b.count && a.last_ts == b.last_ts;
    }

    fn vec_compare(va: &[Bucket], vb: &[Bucket]) -> bool {
        (va.len() == vb.len()) && va.iter().zip(vb).all(|(a, b)| eq_buckets(a, b))
    }

    #[test]
    async fn test_new_bucket_storage() {
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
    async fn test_check_ratelimit_empty_topic_name() {
        let limiter = Limiter::new(vec![Rule {
            max_requests_per_minute: 0,
            topic_name: String::from("a"),
        }]);
        assert_eq!(limiter.check(&String::from("")).unwrap(), true);
    }

    #[test]
    async fn test_check_ratelimit_zero_max_attempts() {
        let limiter = Limiter::new(vec![Rule {
            max_requests_per_minute: 0,
            topic_name: String::from("a"),
        }]);
        assert_eq!(limiter.check(&String::from("a")).unwrap(), true);
    }

    #[test]
    async fn test_check_ratelimit_non_existing_rule() {
        let rules = vec![Rule {
            topic_name: String::from("some_topic_2"),
            max_requests_per_minute: 42,
        }];
        let limiter = Limiter::new(rules);
        assert_eq!(limiter.check(&String::from("some_topic")).unwrap(), true);
    }

    #[test]
    async fn test_check_ratelimit_exceeded() {
        let max_requests = 2;
        let rules = vec![Rule {
            topic_name: String::from("some_name"),
            max_requests_per_minute: max_requests,
        }];
        let limiter = Limiter::new(rules);
        for _ in 0..max_requests {
            assert_eq!(limiter.check(&String::from("some_name")).err(), None);
        }
        let result = limiter.check(&String::from("some_name"));
        assert_eq!(result, Ok(false));
    }

    #[test]
    async fn test_check_ratelimit_passed() {
        let max_requests = 2;
        let rules = vec![Rule {
            topic_name: String::from("some_name"),
            max_requests_per_minute: max_requests,
        }];
        let limiter = Limiter::new(rules);
        for _ in 0..max_requests - 1 {
            assert_eq!(limiter.check(&String::from("some_name")).err(), None);
        }
        let result = limiter.check(&String::from("some_name"));
        assert_eq!(result, Ok(true));
    }

    #[test]
    async fn test_update_bucket() {
        let b = Bucket {
            count: Cell::new(0),
            last_ts: Cell::new(0),
        };
        for _ in 0..42 {
            Limiter::update_bucket(&b, 1);
        }
        assert_eq!(b.last_ts.get(), 1);
        assert_eq!(b.count.get(), 42);
    }

    #[test]
    async fn test_check_locked() {
        //
        let mut bs = BucketStorage::new();
        let max_allowed = 42;
        for _ in 0..max_allowed {
            assert_eq!(Limiter::check_locked(&mut bs.buckets, 1, max_allowed), true);
        }
        assert_eq!(
            Limiter::check_locked(&mut bs.buckets, 1, max_allowed),
            false
        );
    }
}
