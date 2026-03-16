# Service Communication Guide

Kislay Socket is the low-latency transport layer for live service-to-service and browser-to-service communication.

## Namespace

- Primary: `Kislay\\Socket\\Server`
- Compatibility aliases during `0.0.x`: `Kislay\\EventBus\\Server`, `KislayPHP\\EventBus\\Server`, `KislayPHP\\Socket\\Server`

## Pattern

Use explicit channel names:

- `svc.request.<service>` for request messages
- `svc.reply.<service>` for responses
- `evt.<domain>.<event>` for domain fan-out

Include `traceId` and `requestId` in every payload for correlation.

## Minimal Example

See `service_communication.php` in this repository.

## Recommended Cross-Module Setup

1. Use `kislayphp/queue` for durable async jobs.
2. Use this package for low-latency live transport.
3. Keep routing and transport options in `kislayphp/config`.
4. Track connection health with `kislayphp/metrics`.
