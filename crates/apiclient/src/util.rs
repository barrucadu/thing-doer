use reqwest::blocking::Response;
use std::process;

/// If error, print it to stdout and terminate.
pub fn reqwest_error<T>(r: reqwest::Result<T>) -> T {
    match r {
        Ok(t) => t,
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(1);
        }
    }
}

/// Print the response to stderr.
pub fn print_error(response: Response) {
    eprintln!("status: {status}", status = response.status());
    eprintln!("{body}", body = response.text().unwrap());
}
