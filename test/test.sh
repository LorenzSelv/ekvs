curl -i -X GET  10.0.0.2:8080/kvs?key=k1
echo -e "\n===========================\n"
curl -i -X POST 10.0.0.2:8080/kvs -d "key=k1&value=val1"
echo -e "\n===========================\n"
curl -i -X GET  10.0.0.2:8080/kvs?key=k1
echo -e "\n===========================\n"
curl -i -X POST 10.0.0.2:8080/kvs -d "key=k1&value=val2"
echo -e "\n===========================\n"
curl -i -X DELETE 10.0.0.2:8080/kvs?key=k1
echo -e "\n===========================\n"
curl -i -X DELETE 10.0.0.2:8080/kvs?key=k1
echo -e "\n===========================\n"
curl -i -X GET  10.0.0.2:8080/kvs?key=k1
echo -e "\n===========================\n"
curl -i -X GET  10.0.0.2:8080/kvs?key=longggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg
echo -e "\n===========================\n"
curl -i -X GET  10.0.0.2:8080/kvs?key=illegal!
echo -e "\n===========================\n"
