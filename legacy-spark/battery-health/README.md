# legacy-spark/battery-health/

The legacy Scala + sbt Structured Streaming job for battery health. The Scala
is written in stage 3 (risky semantics) and frozen against the pinned Spark
image to produce the golden baseline in stage 4. Guard-protected afterwards.
