import logging
import os
from contextlib import nullcontext

from opentelemetry import metrics, propagate, trace
from opentelemetry.metrics import Histogram
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

LOGGER = logging.getLogger(__name__)
TRACE_CONTEXT_KEYS = {"traceparent", "tracestate", "baggage"}
SECONDS_HISTOGRAM_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    25.0,
    50.0,
    75.0,
    100.0,
)

_configured_services = set()


def configure_otel(service_name):
    """Configure OpenTelemetry exporters for one process and return tracer/meter."""
    if service_name not in _configured_services:
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://observability:4318")
        export_interval = int(os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "5000"))
        resource = Resource.create({
            "service.name": os.environ.get("OTEL_SERVICE_NAME", service_name),
            "service.namespace": "distributed-bookstore",
        })

        try:
            tracer_provider = TracerProvider(resource=resource)
            tracer_provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
            )
            trace.set_tracer_provider(tracer_provider)

            meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[
                    PeriodicExportingMetricReader(
                        OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics"),
                        export_interval_millis=export_interval,
                    )
                ],
                views=[
                    View(
                        instrument_type=Histogram,
                        instrument_name="*_seconds",
                        aggregation=ExplicitBucketHistogramAggregation(
                            boundaries=SECONDS_HISTOGRAM_BUCKETS
                        ),
                    )
                ],
            )
            metrics.set_meter_provider(meter_provider)
            _configured_services.add(service_name)
        except Exception as exc:
            LOGGER.warning("OpenTelemetry setup failed for %s: %s", service_name, exc)

    return trace.get_tracer(service_name), metrics.get_meter(service_name)


def inject_trace_metadata(metadata=()):
    carrier = {}
    propagate.inject(carrier)
    trace_metadata = tuple(
        (key, value) for key, value in carrier.items() if key.lower() in TRACE_CONTEXT_KEYS
    )
    return tuple(metadata) + trace_metadata


def inject_trace_headers(headers=None):
    carrier = dict(headers or {})
    propagate.inject(carrier)
    return carrier


def extract_trace_context(metadata):
    carrier = {
        key: value
        for key, value in metadata
        if key.lower() in TRACE_CONTEXT_KEYS
    }
    return propagate.extract(carrier=carrier)


def safe_attrs(**attrs):
    return {key: value for key, value in attrs.items() if value is not None and value != ""}


def record_exception(span, exc):
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))


def server_span(tracer, context, name, **attrs):
    try:
        parent = extract_trace_context(context.invocation_metadata())
        return tracer.start_as_current_span(
            name,
            context=parent,
            kind=SpanKind.SERVER,
            attributes=safe_attrs(**attrs),
        )
    except Exception:
        return nullcontext()


def client_span(tracer, name, **attrs):
    try:
        return tracer.start_as_current_span(
            name,
            kind=SpanKind.CLIENT,
            attributes=safe_attrs(**attrs),
        )
    except Exception:
        return nullcontext()
