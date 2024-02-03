# 定义循环次数
num_iterations=10

# 循环执行命令
for ((i=0; i<$num_iterations; i++)); do
    # 后台执行命令并将输出追加到日志文件
    # go test -run 2A >> log_${i}.txt &
    go test -run 2B >> log_${i}.txt &
done