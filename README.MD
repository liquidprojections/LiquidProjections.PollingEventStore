# Liquid Projections [![Build status](https://ci.appveyor.com/api/projects/status/5j2rboeh9vg8773w/branch/master?svg=true)](https://ci.appveyor.com/project/dennisdoomen/liquidprojections-pollingeventstore-ctw8n/branch/master)

## What is this?
A source-only Nuget package for event stores that cannot actively push events to [LiquidProjections](https://github.com/liquidprojections/LiquidProjections). It efficiently support multiple concurrent subscribers each interested in a different checkpoint without hitting the underlying event store concurrently.

## Important Notes
* This library uses [Liblog](https://github.com/damianh/LibLog) to log details about asynchronous exceptions to your logging framework of choice. You can also have it log more diagnostic messages by setting the `LIQUIDPROJECTIONS_DIAGNOSTICS` condition symbol.
* If you use this library inside your own packaged library, make sure you set the `LIBLOG_PROVIDERS_ONLY` compiler symbol to prevent the LibLog package from leaking into your consumers. 