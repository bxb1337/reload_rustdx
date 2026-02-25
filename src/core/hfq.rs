use crate::core::qfq::build_qfq_adjusted_prices;
use crate::error::AppResult;
use polars::prelude::DataFrame;

pub(crate) fn build_hfq_adjusted_prices(df: DataFrame) -> AppResult<DataFrame> {
    build_qfq_adjusted_prices(df)
}

#[cfg(test)]
mod tests {
    use super::build_hfq_adjusted_prices;
    use crate::core::qfq::build_qfq_adjusted_prices;
    use polars::prelude::{DataFrame, NamedFrom, Series};

    #[test]
    fn hfq_uses_same_adjustment_series_as_reference_dataset() {
        let rows = 3;
        let df = DataFrame::new(
            rows,
            vec![
                Series::new(
                    "code".into(),
                    vec![
                        "sz000001".to_owned(),
                        "sz000001".to_owned(),
                        "sz000001".to_owned(),
                    ],
                )
                .into(),
                Series::new(
                    "date".into(),
                    vec![
                        "2024-01-29".to_owned(),
                        "2024-01-30".to_owned(),
                        "2024-01-31".to_owned(),
                    ],
                )
                .into(),
                Series::new("open".into(), vec![10.0_f64, 12.0, 15.0]).into(),
                Series::new("high".into(), vec![10.5_f64, 12.5, 15.5]).into(),
                Series::new("low".into(), vec![9.8_f64, 11.8, 14.8]).into(),
                Series::new("close".into(), vec![10.0_f64, 12.0, 15.0]).into(),
                Series::new("volume".into(), vec![10_000_i64, 12_000, 13_000]).into(),
                Series::new("bonus_shares".into(), vec![None, Some(1.0), Some(2.0)]).into(),
                Series::new("cash_dividend".into(), vec![None, Some(1.0), Some(0.5)]).into(),
                Series::new(
                    "rights_issue_shares".into(),
                    vec![None, Some(2.0), Some(1.0)],
                )
                .into(),
                Series::new(
                    "rights_issue_price".into(),
                    vec![None, Some(8.0), Some(9.0)],
                )
                .into(),
            ],
        )
        .expect("build dataframe");

        let hfq = build_hfq_adjusted_prices(df.clone()).expect("build hfq adjusted prices");
        let qfq = build_qfq_adjusted_prices(df).expect("build qfq adjusted prices");

        assert_eq!(hfq.shape(), qfq.shape());
        for column in ["open", "high", "low", "close"] {
            let hfq_values = hfq
                .column(column)
                .expect("hfq column")
                .f64()
                .expect("hfq as f64");
            let qfq_values = qfq
                .column(column)
                .expect("qfq column")
                .f64()
                .expect("qfq as f64");
            for idx in 0..rows {
                let hfq_value = hfq_values.get(idx).expect("hfq value");
                let qfq_value = qfq_values.get(idx).expect("qfq value");
                assert!((hfq_value - qfq_value).abs() < 1e-12);
            }
        }
    }
}
