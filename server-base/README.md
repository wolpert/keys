# Server Base

This is a wrapper to create a Dropwizard server that uses Dagger
for injection instead of spring, and enables metrics from my codehead
metrics library.

# Why does this exist?

It creates a dagger-friendly way to add resources, health checks, etc
if you are not interested in spring. Plus the dropwizard annotated
metrics isn't the best due to name collisions and the difficulties of
adding tags to those annotations. It uses the dropwizard metrics 
integration method for simplicity, given you may want that for JDBI3
and other integrations as well.