num_iterations=10

for ((i=0; i<$num_iterations; i++)); do
    # 循环执行命令并将输出追加到日志文件
    go test -run TestBasic3A >> log_${i}.log
    echo ----------------- ${i} -----------------
    sleep 1
done