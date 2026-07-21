ObjectSerializer Contract
=========================

For every object x:

    loads(dumps(x)) == x

Furthermore:

- dumps(x) MUST return bytes
- loads() MUST accept bytes
- dumps() MUST be deterministic
- loads() MUST never mutate its input