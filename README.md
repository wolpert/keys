# Keys

A complete example of a dropwizard server using the codehead libraries.
This includes:
- metrics
- testing
  - unit testing assist 
  - server-side mocking of dependencies in tests for integ tests (oop-mock)
- feature flags
- simplified extendable dropwizard server with dagger 
- local queues
- state machines
- end-to-end testing

This should provide with a recipe to get started with a new server, with
best practices applied in the software development.

## Side note

Metrics support here is going beyond the dropwizard metrics library. That
library uses annotations to conflict with itself, separating into metric names
where you cannot use counters with timers at the same time. This does make
the system more 'versatile' out of the box. But providing access to the 
underlying micrometer metrics is quite useful, so consider the use of
codehead-metrics as an addon to the existing one... replacing only some of the 
features.
