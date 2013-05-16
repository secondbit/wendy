# Changelog

## Beta1

The first beta release introduces a few changes from the Alpha release:

* Wendy now uses the concept of a neighborhood set to limit the number of Nodes each Node keeps track of. (See [#10](https://github.com/secondbit/wendy/issues/10))
* Wendy now has a better join algorithm, preventing erroneous race condition warnings. (See [#13](https://github.com/secondbit/wendy/issues/13))
* Wendy now has some end-to-end integration tests that cover the joining algorithm for Nodes. (See [#16](https://github.com/secondbit/wendy/issues/16))
* Wendy now uses state table versioning instead of timestamps to detect race conditions, removing the dependency on the Nodes' clocks being in sync. (See [#4](https://github.com/secondbit/wendy/issues/4))
* Wendy now keeps track of the bound port, when ports are auto-assigned. (See [#17](https://github.com/secondbit/wendy/issues/17)) (Courtesy of [Graeme Humphries](https://github.com/unit3))
