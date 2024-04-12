flink run -yd -t yarn-per-job --detached \
  -ytm 1024 \
  -yjm 1024  \
  -yD state.checkpoints.dir="./tmp/checkpoint" \
  -yD execution.checkpointing.interval="1 min" \
  -ynm OlympicApp \
  -c com.rakudo.StreamingApp target/flink-template-1.0.jar