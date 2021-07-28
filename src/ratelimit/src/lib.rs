#[derive(Debug, Clone)]
pub struct Rule {
    pub topic_name: String,
    pub max_requests_per_second: u32,
}

#[derive(Debug)]
struct Bucket {
    last_ts: i32,
    count: i32,
}

struct BucketStorage {
    buckets: std::vec::Vec<Bucket>,
}

impl BucketStorage {
    pub fn new() -> BucketStorage {
        let mut v = std::vec::Vec::new();
        for _ in 0..BUCKET_COUNT {
            v.push(Bucket {
                count: 0,
                last_ts: 0,
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

const BUCKET_COUNT: i32 = 60;

impl Limiter {
    pub fn check(&self, topic_name: &String) -> Result<bool, str> {
        if topic_name == "" {
            // NOTE(shmel1k): if topic_name is empty, ratelimit is not checked.
            return Ok(true);
        }

        let rule = self.rules.get(topic_name);
        let max_attempts = match rule {
            Some(t) => t.max_requests_per_second,
            None => return true,
        };

        if max_attempts == 0 {
            return true;
        }

        let lock = self.container.lock();
        match lock {
            Err(e) =>
        }

        false
    }

    fn calculate_bucket(ts: i32) -> i32 {
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
                count: 0,
                last_ts: 0,
            });
        }

        assert_eq!(vec_compare(&b.buckets, &v), true);
        assert_eq!(b.buckets.len(), 60);
    }

    #[test]
    fn test_check_ratelimit_empty_topic_name() {
        let limiter = Limiter::new(vec![Rule {
            max_requests_per_second: 0,
            topic_name: String::from("a"),
        }]);
        assert_eq!(limiter.check(&String::from("")), true);
    }

    #[test]
    fn test_check_ratelimit_zero_max_attempts() {
        let limiter = Limiter::new(vec![Rule {
            max_requests_per_second: 0,
            topic_name: String::from("a"),
        }]);
        assert_eq!(limiter.check(&String::from("a")), true);
    }

    #[test]
    fn test_check_ratelimit_no_rule() {
        let mut rules = vec![Rule {
            topic_name: String::from("some_topic"),
            max_requests_per_second: 42,
        }];
        let limiter = Limiter::new(rules);
        limiter.check(&String::from("some_topic"));
    }
}
