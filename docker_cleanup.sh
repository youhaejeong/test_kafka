#!/bin/bash

#############################
# docker compose 완전 정리~~ #
#############################

# 백그라운드 컨테이너 중지
echo "stop background container"
docker compose down -v --remove-orphans

# 사용하지 않는 이미지,컨테이너,네트워크,불륨 정리
echo "clean docker system"
docker system prune -a --volumes -f

# 완료
echo "complete clean docker compose"