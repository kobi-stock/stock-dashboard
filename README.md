# 📈 Stock Dashboard (KIS 기반 실시간 주식 앱)

한국투자증권(KIS) API와 네이버 API를 이용한  
실시간 주식 대시보드 웹 애플리케이션입니다.

---

## 🚀 주요 기능

- 📊 국내 주식 실시간 조회 (KIS API)
- 🌎 해외 주식 조회 (yfinance)
- 💼 투자 포트폴리오 관리
- 📰 뉴스 조회 (네이버 API)
- ⚡ WebSocket 기반 실시간 반영

---

## 🛠️ 실행 방법

### 1️⃣ 프로젝트 클론

```
git clone https://github.com/your-id/stock-dashboard.git
cd stock-dashboard
```

---

### 2️⃣ 환경변수 설정

backend 폴더에 `.env` 파일 생성:

```
KIS_APP_KEY=your_kis_app_key
KIS_APP_SECRET=your_kis_app_secret
NAVER_CLIENT_ID=your_naver_client_id
NAVER_CLIENT_SECRET=your_naver_client_secret
```

---

### 3️⃣ 백엔드 실행

```
cd backend
pip install -r requirements.txt
py -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

---

### 4️⃣ 프론트 실행

```
cd frontend
npm install
npm run dev
```

---

## 🌐 접속

```
http://localhost:5173
```

---

## ⚠️ 주의사항

- `.env` 파일은 절대 GitHub에 업로드하지 마세요
- KIS API 키는 개인별로 발급받아야 합니다

---

## 📌 기술 스택

- Backend: FastAPI
- Frontend: React (Vite)
- API: KIS Open API, Naver API
- Realtime: WebSocket

---

## 🎯 향후 개선

- NXT 애프터마켓 반영
- 호가창 UI 추가
- 모바일 UI 최적화
