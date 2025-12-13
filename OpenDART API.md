# OpenDART API 개발가이드 정리

본 문서는 다음 4개 개발가이드를 **Markdown 문법**으로 통합·정리한 자료입니다.

- 고유번호 개발가이드
- 공시검색 개발가이드
- 다중회사 주요계정 개발가이드
- 주식의 총수 현황 개발가이드

---

## 1. 고유번호 개발가이드

### 1.1 기본 정보

| 항목      | 내용                                          |
| --------- | --------------------------------------------- |
| 메서드    | `GET`                                         |
| 요청 URL  | `https://opendart.fss.or.kr/api/corpCode.xml` |
| 인코딩    | UTF-8                                         |
| 출력 포맷 | Zip FILE (binary)                             |

---

### 1.2 요청 인자

| 요청키      | 명칭       | 타입       | 필수 | 설명                     |
| ----------- | ---------- | ---------- | ---- | ------------------------ |
| `crtfc_key` | API 인증키 | STRING(40) | Y    | 발급받은 인증키 (40자리) |

---

### 1.3 응답 결과 (ZIP 내 XML)

| 응답키          | 설명                     |
| --------------- | ------------------------ |
| `status`        | 에러 및 정보 코드        |
| `message`       | 에러 및 정보 메시지      |
| `corp_code`     | 고유번호 (8자리)         |
| `corp_name`     | 정식 회사명              |
| `corp_eng_name` | 영문 정식 회사명         |
| `stock_code`    | 종목코드 (상장사, 6자리) |
| `modify_date`   | 최종 변경일자 (YYYYMMDD) |

---

### 1.4 메시지 코드

- `000` 정상
- `010` 등록되지 않은 키
- `011` 사용할 수 없는 키
- `012` 접근할 수 없는 IP
- `013` 조회 데이터 없음
- `014` 파일 미존재
- `020` 요청 제한 초과
- `021` 조회 회사 수 초과 (최대 100건)
- `100` 필드 값 오류
- `101` 부적절한 접근
- `800` 시스템 점검
- `900` 정의되지 않은 오류
- `901` 개인정보 보유기간 만료

---

## 2. 공시검색 개발가이드

### 2.1 기본 정보

| 항목            | 내용                                       |
| --------------- | ------------------------------------------ |
| 메서드          | `GET`                                      |
| 요청 URL (JSON) | `https://opendart.fss.or.kr/api/list.json` |
| 요청 URL (XML)  | `https://opendart.fss.or.kr/api/list.xml`  |
| 인코딩          | UTF-8                                      |
| 출력 포맷       | JSON / XML                                 |

---

### 2.2 요청 인자

| 요청키             | 타입       | 필수 | 설명                    |
| ------------------ | ---------- | ---- | ----------------------- |
| `crtfc_key`        | STRING(40) | Y    | API 인증키              |
| `corp_code`        | STRING(8)  | N    | 회사 고유번호           |
| `bgn_de`           | STRING(8)  | N    | 검색 시작일 (YYYYMMDD)  |
| `end_de`           | STRING(8)  | N    | 검색 종료일 (YYYYMMDD)  |
| `last_reprt_at`    | STRING(1)  | N    | 최종보고서 여부 (Y/N)   |
| `pblntf_ty`        | STRING(1)  | N    | 공시유형 (A~J)          |
| `pblntf_detail_ty` | STRING(4)  | N    | 공시 상세유형           |
| `corp_cls`         | STRING(1)  | N    | 법인구분 (Y/K/N/E)      |
| `sort`             | STRING(4)  | N    | 정렬기준 (date/crp/rpt) |
| `sort_mth`         | STRING(4)  | N    | 정렬방식 (asc/desc)     |
| `page_no`          | STRING(5)  | N    | 페이지 번호             |
| `page_count`       | STRING(3)  | N    | 페이지당 건수 (1~100)   |

---

### 2.3 응답 결과

| 응답키        | 설명                             |
| ------------- | -------------------------------- |
| `page_no`     | 페이지 번호                      |
| `page_count`  | 페이지별 건수                    |
| `total_count` | 전체 건수                        |
| `total_page`  | 전체 페이지 수                   |
| `corp_cls`    | 법인구분                         |
| `corp_name`   | 회사명                           |
| `corp_code`   | 고유번호                         |
| `stock_code`  | 종목코드                         |
| `report_nm`   | 보고서명                         |
| `rcept_no`    | 접수번호 (14자리)                |
| `rcept_dt`    | 접수일자                         |
| `flr_nm`      | 제출인명                         |
| `rm`          | 비고 (유, 코, 채, 연, 정, 철 등) |

---

## 3. 다중회사 주요계정 개발가이드

### 3.1 기본 정보

| 항목            | 내용                                                 |
| --------------- | ---------------------------------------------------- |
| 메서드          | `GET`                                                |
| 요청 URL (JSON) | `https://opendart.fss.or.kr/api/fnlttMultiAcnt.json` |
| 요청 URL (XML)  | `https://opendart.fss.or.kr/api/fnlttMultiAcnt.xml`  |

---

### 3.2 요청 인자

| 요청키       | 타입       | 필수 | 설명                      |
| ------------ | ---------- | ---- | ------------------------- |
| `crtfc_key`  | STRING(40) | Y    | API 인증키                |
| `corp_code`  | STRING(8)  | Y    | 회사 고유번호             |
| `bsns_year`  | STRING(4)  | Y    | 사업연도 (2015년 이후)    |
| `reprt_code` | STRING(5)  | Y    | 보고서 코드 (11011~11014) |

---

### 3.3 응답 주요 필드

| 필드               | 설명                  |
| ------------------ | --------------------- |
| `account_nm`       | 계정명 (예: 자본총계) |
| `fs_div`           | OFS / CFS             |
| `sj_div`           | BS / IS               |
| `thstrm_amount`    | 당기 금액             |
| `frmtrm_amount`    | 전기 금액             |
| `bfefrmtrm_amount` | 전전기 금액           |
| `currency`         | 통화 단위             |

---

## 4. 주식의 총수 현황 개발가이드

### 4.1 기본 정보

| 항목            | 내용                                                  |
| --------------- | ----------------------------------------------------- |
| 메서드          | `GET`                                                 |
| 요청 URL (JSON) | `https://opendart.fss.or.kr/api/stockTotqySttus.json` |
| 요청 URL (XML)  | `https://opendart.fss.or.kr/api/stockTotqySttus.xml`  |

---

### 4.2 요청 인자

| 요청키       | 타입       | 필수 | 설명          |
| ------------ | ---------- | ---- | ------------- |
| `crtfc_key`  | STRING(40) | Y    | API 인증키    |
| `corp_code`  | STRING(8)  | Y    | 회사 고유번호 |
| `bsns_year`  | STRING(4)  | Y    | 사업연도      |
| `reprt_code` | STRING(5)  | Y    | 보고서 코드   |

---

### 4.3 응답 주요 필드

| 필드                      | 설명                    |
| ------------------------- | ----------------------- |
| `isu_stock_totqy`         | 발행할 주식의 총수      |
| `now_to_isu_stock_totqy`  | 현재까지 발행한 주식 수 |
| `now_to_dcrs_stock_totqy` | 현재까지 감소한 주식 수 |
| `istc_totqy`              | 발행주식 총수           |
| `tesstk_co`               | 자기주식 수             |
| `distb_stock_co`          | 유통주식 수             |
| `stlm_dt`                 | 결산기준일              |

---

## 5. 공통 에러 코드 요약

- `000` 정상
- `010~014` 인증/파일/데이터 오류
- `020~021` 요청/조회 제한 초과
- `100~101` 요청 값 오류
- `800` 시스템 점검
- `900~901` 시스템/계정 오류

---

### 참고

- 접수번호를 이용한 공시 뷰어 연결
  `https://dart.fss.or.kr/dsaf001/main.do?rcpNo=접수번호`
