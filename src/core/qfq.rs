use crate::error::{AppResult, OutputError};
use polars::prelude::{DataFrame, IntoLazy, NamedFrom, Series, SortMultipleOptions, col};

pub(crate) fn build_qfq_adjusted_prices(df: DataFrame) -> AppResult<DataFrame> {
    let mut sorted = df
        .lazy()
        .sort(["code", "date"], SortMultipleOptions::default())
        .collect()
        .map_err(OutputError::BuildDataFrame)?;

    let row_count = sorted.height();
    let code_values = sorted
        .column("code")
        .map_err(OutputError::BuildDataFrame)?
        .str()
        .map_err(OutputError::BuildDataFrame)?;
    let close_values = sorted
        .column("close")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let open_values = sorted
        .column("open")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let high_values = sorted
        .column("high")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let low_values = sorted
        .column("low")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let cash_values = sorted
        .column("cash_dividend")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let bonus_values = sorted
        .column("bonus_shares")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let rights_values = sorted
        .column("rights_issue_shares")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;
    let rights_price_values = sorted
        .column("rights_issue_price")
        .map_err(OutputError::BuildDataFrame)?
        .f64()
        .map_err(OutputError::BuildDataFrame)?;

    let closes = close_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let opens = open_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let highs = high_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let lows = low_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let cash_dividend = cash_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let bonus_shares = bonus_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let rights_issue_shares = rights_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();
    let rights_issue_price = rights_price_values
        .into_iter()
        .map(|value| value.unwrap_or(0.0))
        .collect::<Vec<_>>();

    let mut adjusted_open = opens.clone();
    let mut adjusted_high = highs.clone();
    let mut adjusted_low = lows.clone();
    let mut adjusted_close = closes.clone();
    let mut start = 0_usize;
    while start < row_count {
        let mut end = start + 1;
        while end < row_count && code_values.get(end) == code_values.get(start) {
            end += 1;
        }

        let mut cumulative_dividend = 0.0;
        let mut cumulative_bonus = 0.0;
        let mut cumulative_rights = 0.0;
        let mut cumulative_rights_price = 0.0;

        for idx in (start..end.saturating_sub(1)).rev() {
            let next_idx = idx + 1;
            let alpha = cash_dividend[next_idx];
            let beta = bonus_shares[next_idx];
            let gamma = rights_issue_shares[next_idx];
            let epsilon = rights_issue_price[next_idx];

            cumulative_dividend =
                cumulative_dividend * (1.0 + 0.1 * beta) * (1.0 + 0.1 * gamma) + alpha;
            cumulative_bonus = cumulative_bonus * (1.0 + 0.1 * beta) + beta;
            cumulative_rights = cumulative_rights * (1.0 + 0.1 * gamma) + gamma;
            cumulative_rights_price =
                cumulative_rights_price * (1.0 + 0.1 * beta) * (1.0 + 0.1 * gamma) + epsilon;

            let denominator = (1.0 + 0.1 * cumulative_bonus) * (1.0 + 0.1 * cumulative_rights);
            let adjustment_term =
                -0.1 * cumulative_dividend + cumulative_rights_price * 0.1 * cumulative_rights;

            adjusted_open[idx] = (opens[idx] + adjustment_term) / denominator;
            adjusted_high[idx] = (highs[idx] + adjustment_term) / denominator;
            adjusted_low[idx] = (lows[idx] + adjustment_term) / denominator;
            adjusted_close[idx] = (closes[idx] + adjustment_term) / denominator;
        }

        start = end;
    }

    sorted
        .with_column(Series::new("open".into(), adjusted_open).into())
        .map_err(OutputError::BuildDataFrame)?;
    sorted
        .with_column(Series::new("high".into(), adjusted_high).into())
        .map_err(OutputError::BuildDataFrame)?;
    sorted
        .with_column(Series::new("low".into(), adjusted_low).into())
        .map_err(OutputError::BuildDataFrame)?;
    sorted
        .with_column(Series::new("close".into(), adjusted_close).into())
        .map_err(OutputError::BuildDataFrame)?;

    sorted
        .lazy()
        .select([
            col("code"),
            col("date"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("bonus_shares"),
            col("cash_dividend"),
            col("rights_issue_shares"),
            col("rights_issue_price"),
        ])
        .collect()
        .map_err(OutputError::BuildDataFrame)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::build_qfq_adjusted_prices;
    use polars::prelude::{DataFrame, NamedFrom, Series};

    #[test]
    fn qfq_rights_issue_price_accumulates_with_bonus_and_rights_factors() {
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

        let adjusted = build_qfq_adjusted_prices(df).expect("build qfq adjusted prices");
        let closes = adjusted
            .column("close")
            .expect("close column")
            .f64()
            .expect("close as f64");

        let first_close = closes.get(0).expect("first close exists");
        let second_close = closes.get(1).expect("second close exists");
        let third_close = closes.get(2).expect("third close exists");

        let expected_first = (10.0 - 0.1 * 1.66 + 19.88 * 0.1 * 3.2) / (1.32 * 1.32);
        let expected_second = (12.0 - 0.1 * 0.5 + 9.0 * 0.1 * 1.0) / (1.2 * 1.1);

        assert!((first_close - expected_first).abs() < 1e-12);
        assert!((second_close - expected_second).abs() < 1e-12);
        assert!((third_close - 15.0).abs() < 1e-12);
    }
}
