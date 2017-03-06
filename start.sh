nohup java -Xms8G -Xmx8G -XX:+UseCompressedOops -XX:MaxDirectMemorySize=6G \
 -Dpcc.data.dir="/data/tangfl/pcc/data" -Dlog.home="/data/tangfl/pcc" \
 -jar chestnut-2.0.jar > stdout.log 2>&1 &
