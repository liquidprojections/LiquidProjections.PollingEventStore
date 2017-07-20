# Liquid Projections [![Build status](https://ci.appveyor.com/api/projects/status/85nutadfiq44bwq1?svg=true)](https://ci.appveyor.com/project/dennisdoomen/liquidprojections.pollingeventstore)

## What is this?
An adapter for event stores that cannot actively push events to [LiquidProjections](https://github.com/liquidprojections/LiquidProjections). It efficiently support multiple concurrent subscribers each interested in a different checkpoint without hitting the underlying event store concurrently