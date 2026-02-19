# Synapse Framework

## Overview

Synapse is a highly efficient and pluggable, open-source crawling/scraping framework;
for both local and distributed workloads.

There're two integration paths, based on level of control:

1. High-Level API: Built for standard crawling workloads.
   Extend with built-in plugins and get moving immediately without fiddling
   with the underlying mechanics. **[TODO]**

2. Low-Level API: For architecting custom scrapers/crawlers with (Sub)component-level control.
   Extend with your own implementations. **[WIP]**

## Status

The distributed architecture is **[WIP]**; essentially tinkering with distributed state-machine.
Currently, in experimental phase. Expect breaking changes as the architecture evolves.

## Documentation

Efforts are currently prioritized toward solid core abstractions over polished public documentation.
Implementation-specific details are available within each component's directory for developers
diving into the internals.

## Development

Contributions are welcome!

- Start by checking [contribution guidelines](./CONTRIBUTING.md).

- Any questions, ask on [discussions](https://github.com/vyrelabs/synapse/discussions)

## Why this naming?

In neurobiology, a synapse is the junction for signal transmission between neurons. 
This framework serves as the interface between the web and application-specific logic, 
decoupling data acquisition from downstream processing.

## Ethical Considerations

It's not intended for any malicious or unethical web scraping/crawling activities.
Please ensure you [comply with the website's `robots.txt` directives](./frontier/robots)
and terms of service (TOS) before crawling/scraping.

