use std::cmp::Ord;

use itertools::Itertools;

pub fn mapreduce<
    InT,
    InIterator: IntoIterator<Item = InT>,
    MappedT,
    ReducedT,
    MappedKey: Ord,
    Mapper: Fn(InT) -> Vec<(MappedKey, MappedT)>,
    Reducer: Fn(&MappedKey, Vec<MappedT>) -> Vec<ReducedT>,
>(
    lines: InIterator,
    mapper: Mapper,
    reducer: Reducer,
) -> Vec<ReducedT>
where
    InT: Send,
    MappedKey: Clone + Send,
    MappedT: Send,
    Mapper: Send,
{
    let mut mapped = Vec::new();

    for item in lines {
        for (mapped_key, mapped_value) in mapper(item) {
            mapped.push((mapped_key, mapped_value));
        }
    }
    // sort it in-place
    mapped.sort_unstable_by(
        |&(ref key1, ref _val1), &(ref key2, ref _val2)| key1.cmp(key2),
    );

    let mut ret = vec![];

    for (key, items) in &mapped.into_iter().group_by(|ref tup| tup.0.clone()) {
        for reduced in reducer(&key, items.map(|(_k, v)| v).collect()) {
            ret.push(reduced);
        }
    }

    return ret;
}

#[cfg(test)]
mod tests {
    use super::mapreduce;

    #[test]
    fn it_works() {
        let mut result: Vec<(bool, u32)> = mapreduce(
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            |num| {
                let ret: Vec<(bool, u32)> = vec![(num % 2 == 0, num * 2)];
                ret
            },
            |&evenness, nums| {
                let mut sum = 0;
                for int in nums {
                    sum += int;
                }
                vec![(evenness, sum)]
            },
        );
        result.sort();
        assert_eq!(result, vec![(false, 50), (true, 40)]);
    }
}
