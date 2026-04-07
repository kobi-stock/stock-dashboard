import json

# 기존 파일 읽기
with open("kis_master.json", "r", encoding="utf-8") as f:
    raw = json.load(f)

result = []

for code, value in raw.items():
    # ISIN (앞 12자리) 제거하고 이름만 추출
    name = value[12:]

    result.append({
        "code": code,
        "name": name
    })

# 새 파일 저장
with open("kis_master_converted.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("변환 완료!")