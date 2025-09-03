use fred::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;


pub struct PyRedisError(RedisError);

impl From<PyRedisError> for PyErr {
    fn from(error: PyRedisError) -> Self {
        PyRuntimeError::new_err(error.0.to_string())
    }
}

impl From<RedisError> for PyRedisError {
    fn from(other: RedisError) -> Self {
        Self(other)
    }
}

#[pyclass(name = "Client")]
struct PyRedisClient {
    client: RedisClient,
}

#[pymethods]
impl PyRedisClient {
    #[new]
    #[pyo3(signature=(uri=None))]
    fn new(uri: Option<&str>) -> PyResult<Self> {
        let config = match uri {
            Some(uri) => RedisConfig::from_url_centralized(uri).map_err(PyRedisError::from)?,
            None => RedisConfig::default(),
        };
        let perf = PerformanceConfig::default();
        let connection = ConnectionConfig::default();
        let policy = ReconnectPolicy::default();
        Ok(Self {
            client: RedisClient::new(config, Some(perf), Some(connection), Some(policy)),
        })
    }

    fn connect<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            client.connect(); // While not async, it needs the reactor running.
            client
                .wait_for_connect()
                .await
                .map_err(PyRedisError::from)?;
            Ok(())
        })
    }

    pub fn ping<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            client.ping::<()>().await.map_err(PyRedisError::from)?;
            Ok(())
        })
    }
}

#[pymodule]
fn pyfred(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRedisClient>()?;
    Ok(())
}
