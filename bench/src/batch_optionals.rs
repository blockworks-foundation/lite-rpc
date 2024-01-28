use std::slice::Iter;
use itertools::Itertools;

pub struct BatchOptionals<T, E> {
    ok_values: Vec<T>,
    error_values: Vec<E>,
    error_indices: Vec<usize>,
}

impl <T, E> BatchOptionals<T, E> {
    pub fn new(input: Vec<Result<T, E>>) -> Self {
        let (some_values, error_values, none_indices) = Self::map_batch_optionals(input);
        Self {
            ok_values: some_values,
            error_values: error_values,
            error_indices: none_indices,
        }
    }

    fn map_batch_optionals(input: Vec<Result<T, E>>) -> (Vec<T>, Vec<E>, Vec<usize>) {
        input.into_iter().enumerate()
            .fold((vec![], vec![], vec![]), |(mut output, mut errout, mut none_indices), (i, x)| {
                match x {
                    Ok(x) => output.push(x),
                    Err(e) => { errout.push(e); none_indices.push(i); },
                }
                (output, errout, none_indices)
            })
    }

    pub fn view_okeys(&self) -> Iter<'_, T> {
        self.ok_values.iter()
    }

    pub fn uncompress<A>(self, okeys: Vec<A>) -> Vec<Result<A, E>> {
        let mut out = okeys.into_iter().map(Ok).collect_vec();
        assert_eq!(self.error_indices.len(), self.error_values.len());
        self.error_values.into_iter().enumerate().for_each(|(i, e)| {
            out.insert(self.error_indices[i], Err(e));
        });
        out
    }

}


#[test]
pub fn simple() {
    let input = vec![Ok(10), Err(()), Ok(20), Err(()), Ok(30)];

    let batch_optionals: BatchOptionals<i32,()> = BatchOptionals::new(input.clone());

    assert_eq!(batch_optionals.view_okeys().collect_vec(), vec![&10, &20, &30]);

    let overlay = vec!["10", "20", "30"];
    let out = batch_optionals.uncompress(overlay);

    assert_eq!(out, vec![Ok("10"), Err(()), Ok("20"), Err(()), Ok("30")]);

}