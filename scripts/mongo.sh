#!/usr/bin/env bash
# Mở MongoDB shell (mongosh) vào database joblens bằng user ứng dụng (readWrite chỉ trên joblens, không dùng root).
#   bash scripts/mongo.sh                                   # phiên tương tác: gõ lệnh tự do, thoát: exit
#   bash scripts/mongo.sh 'db.province_windows.countDocuments()'   # chạy một lệnh rồi thoát
# Ví dụ trong phiên tương tác:
#   show collections
#   db.sandbox_jobs.insertOne({_id: "t1", title: "Tin thử", location_code: "01", salary_max_vnd: 20000000})
#   db.sandbox_jobs.updateOne({_id: "t1"}, {$set: {salary_max_vnd: 25000000}})
#   db.sandbox_jobs.deleteOne({_id: "t1"})
# Collection do pipeline ghi (province_windows, analytics_*) sẽ bị job ghi đè – thử nghiệm trên collection riêng.
set -euo pipefail
URL=$(kubectl get secret joblens-mongo -n serving -o jsonpath='{.data.JOBLENS_MONGO_URL}' | base64 -d)
if [ $# -eq 0 ]; then
  exec kubectl exec -it -n serving mongodb-0 -- mongosh --quiet "$URL"
else
  kubectl exec -n serving mongodb-0 -- mongosh --quiet "$URL" --eval "$1"
fi
