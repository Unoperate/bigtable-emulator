# bazel run //:clear
# clear && bazel build //... && ./bazel-bin/emulator

./populate_db_small.sh

export BIGTABLE_EMULATOR_HOST=localhost:8888

cbt -project=p -instance=i count cars # 2

cbt -project=p -instance=i deletefamily cars id
cbt -project=p -instance=i deletefamily cars params

cbt -project=p -instance=i count cars # 0