use crate::app::AppState;
use crate::auth::AuthenticatedUser;
use actix_web::{
    error::ErrorBadRequest,
    error::ErrorNotFound,
    error::PayloadError,
    http::StatusCode,
    post,
    web::{Data, Json, Path, Payload, Query},
    HttpResponse, Responder, ResponseError,
};
use futures::{StreamExt, TryStreamExt};
use ipp::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::io::StreamReader;

#[derive(Serialize, Debug, Clone)]
struct SuccessReply {
    message: &'static str,
    job_link: Option<String>,
    job_id: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ParseRangeError {
    error: String,
    bad_range: String,
}

impl Display for ParseRangeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Range Parsing Error: {} in {}",
            self.error, self.bad_range
        )
    }
}
impl std::error::Error for ParseRangeError {}

#[derive(thiserror::Error, Debug)]
pub enum KprintError {
    #[error("an unspecified internal error occurred: {0}")]
    InternalError(#[from] anyhow::Error),
    #[error("Actix error: {0}")]
    Actix(#[from] actix_web::error::Error),
    #[error("Page range parse failure: {0}")]
    PageRange(#[from] ParseRangeError),
}

impl ResponseError for KprintError {
    fn status_code(&self) -> StatusCode {
        match &self {
            Self::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Actix(err) => err.as_response_error().status_code(),
            Self::PageRange(_) => StatusCode::PRECONDITION_FAILED,
        }
    }

    fn error_response(&self) -> HttpResponse {
        if let Self::Actix(err) = self {
            err.as_response_error().error_response()
        } else {
            HttpResponse::build(self.status_code()).body(self.to_string())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum DuplexMode {
    TwoSidedLongEdge,
    TwoSidedShortEdge,
    OneSided,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ColorMode {
    Monochrome,
    Color,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrintOptions {
    // "sides": A keyword that specifies whether to do two sided printing. Values include 'one-sided', 'two-sided-long-edge' (typical 2-sided printing for portrait Documents), and 'two-sided-short-edge' (2-sided printing for landscape Documents).
    sides: DuplexMode,
    // "print-color-mode": A keyword specifying the color printing mode to use. The value 'color' specifies a full-color print, 'monochrome' specifies a grayscale print, and 'bi-level' specifies a black-and-white (no shades of gray) print.
    color_mode: ColorMode,
    pages: String,
    copies: u32,
    title: String,
}

fn parse_one_in_range(term: &str) -> Result<i32, ParseRangeError> {
    term.parse::<i32>().map_err(|error| ParseRangeError {
        error: error.to_string(),
        bad_range: term.to_string(),
    })
}

fn parse_range(range: &str) -> Result<(i32, i32), ParseRangeError> {
    let (start, end) = range.split_once('-').unwrap_or((range, range));
    log::debug!("Parsing a range between {start:?} and {end:?}");
    let (start, end) = (parse_one_in_range(start)?, parse_one_in_range(end)?);
    // IPP pages start at 1 and a range can't run backwards
    if start < 1 || end < start {
        return Err(ParseRangeError {
            error: "pages start at 1 and end must be >= start".to_string(),
            bad_range: range.to_string(),
        });
    }
    Ok((start, end))
}

#[post("/printers/{printer}/print")]
pub async fn print(
    printer: Path<String>,
    app_data: Data<AppState>,
    user: AuthenticatedUser,
    Query(options): Query<PrintOptions>,
    payload: Payload,
) -> Result<impl Responder, KprintError> {
    log::debug!(
        "Got a print request from {}",
        user.claims.preferred_username().unwrap().as_str()
    );
    let printer = match app_data.printers.get(&*printer) {
        Some(printer) => printer,
        None => {
            return Err(ErrorNotFound(format!("Printer named {printer} doesn't exist!")).into())
        }
    };

    if !(1..100).contains(&options.copies) {
        return Err(ErrorBadRequest("copies must be 1-100").into()); //DONT SPAM
    }

    // Empty string is the same as all pages
    let mut page_ranges = if !options.pages.trim().is_empty() {
        options
            .pages
            .split(',')
            .map(|term| parse_range(term.trim()))
            .collect::<Result<Vec<(i32, i32)>, ParseRangeError>>()?
    } else {
        vec![]
    };
    page_ranges.sort_by_key(|(start, _end)| *start);
    let page_ranges = page_ranges
        .into_iter()
        .coalesce(|(prev_start, prev_end), (this_start, this_end)| {
            if prev_end >= this_start {
                // Merge the two ranges if they have overlap
                Ok((prev_start, prev_end.max(this_end)))
            } else {
                Err(((prev_start, prev_end), (this_start, this_end)))
            }
        })

    .map(|(min,max)| IppValue::RangeOfInteger { min, max }).collect::<Vec<_>>();

    let page_ranges = if page_ranges.is_empty() {
        vec![]
    } else {
        vec![IppAttribute::new(
            "page-ranges",
            IppValue::Array(page_ranges),
        )]
    };

    log::debug!("Here's where we landed with panges: {page_ranges:?}");

    let (tx, rx) = futures::channel::mpsc::channel(1);
    actix_web::rt::spawn(async move {
        if let Err(err) = payload
            .map_err(|err| match err {
                PayloadError::Incomplete(Some(err)) | PayloadError::Io(err) => err,
                other => std::io::Error::other(other),
            })
            .map(Ok)
            .forward(tx)
            .await
        {
            log::warn!("Hung up! Cancelling the reader! {err}");
        }
    });

    let payload = IppPayload::new_async(StreamReader::new(rx).compat());

    let operation = IppOperationBuilder::print_job(printer.uri().clone(), payload)
        .user_name(user.claims.preferred_username().unwrap().as_str())
        .job_title(options.title)
        .attribute(IppAttribute::new(
            "sides",
            IppValue::Keyword(
                serde_variant::to_variant_name(&options.sides)
                    .unwrap()
                    .to_string(),
            ),
        ))
        .attribute(IppAttribute::new(
            "print-color-mode",
            IppValue::Keyword(
                serde_variant::to_variant_name(&options.color_mode)
                    .unwrap()
                    .to_string(),
            ),
        ))
        .attributes(page_ranges)
        .attribute(IppAttribute::new(
            "copies",
            IppValue::Integer(options.copies as i32),
        ))
        .build();

    log::debug!("Sending operation to printer!");

    let response = printer.send(operation).await.map_err(|err| {
        log::error!("IPP send failed!: {err:?}");
        anyhow::Error::from(err)
    })?;

    if !response.header().status_code().is_success() {
        log::error!("printer rejected job: {:?}", response.header());
        return Err(anyhow::anyhow!(
                "printer rejected job!: {}",
                response.header().status_code()
        )
            .into());
    }

    let attributes = response.attributes();
    let job_link = attributes
        .groups()
        .iter()
        .find_map(|group| group.attributes().get("job-uri"))
        .and_then(|job_uri| {
            job_uri
                .value()
                .as_uri()
                .and_then(|uri| Uri::from_str(uri).ok())
        })
        .map(|job_uri| {
            let mut parts = job_uri.into_parts();
            parts.scheme = Some("https".parse().unwrap());
            Uri::from_parts(parts).unwrap()
        })
        .map(|uri| uri.to_string());
    let job_id = attributes
        .groups()
        .iter()
        .find_map(|group| group.attributes().get("job-id"))
        .and_then(|job_uri| job_uri.value().as_integer().copied());

    log::debug!(
        "Reply from print server! Header: {:?} Attributes {:?} Payload {:?}",
        response.header(),
        response.attributes(),
        response.to_bytes()
    );
    Ok(Json(SuccessReply {
        message: "lmao",
        job_link,
        job_id,
    }))
}
