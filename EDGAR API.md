# EDGAR API Development Toolkit
## 1. 개요 (Overview)

이 문서는 **EDGAR 제출자(filer)**가 다음 작업을 수행할 수 있도록 지원하는 **EDGAR API** 사용 방법을 설명합니다.

* EDGAR 시스템 상태 확인
* 사용자 및 권한 관리
* EDGAR 서류 제출
* 제출된 서류의 상태 확인

> **참고**
> 기업별 EDGAR 제출 데이터나 XBRL 데이터 접근은 `data.sec.gov` 관련 리소스를 참고해야 합니다.

### 기술 개요

* REST 기반 API
* 요청/응답: JSON
* 제출 요청: Binary 또는 XML
* HTTP 표준 메서드 및 IANA 상태 코드 사용

---

## 2. 인증 토큰 (Tokens)

EDGAR API는 **HTTP Bearer 인증**을 사용합니다.
토큰은 **Filer Management**에서 발급되며, 두 종류가 있습니다.

### 2.1 토큰 종류

| 구분                  | 설명           |
| ------------------- | ------------ |
| **Filer API Token** | 조직(법인) 단위 토큰 |
| **User API Token**  | 개인 사용자 단위 토큰 |

* 토큰은 **JWT (JOSE 형식)**
* Payload는 **암호화 + 서명**
* SEC는 토큰 내용을 요구하지 않음

---

## 3. JWT 토큰 구조

JWT는 `.`으로 구분된 Base64 인코딩 스탠자로 구성됩니다.

### 3.1 Header 예시 (Filer API Token)

```json
{
  "cik": "0000000000",
  "kid": "75c8d379-2d82-415e-a3c5-7501699e3101",
  "alg": "ECDH-ES",
  "expiresAt": "2025-07-25T03:00:00Z"
}
```

#### 주요 필드 설명

| 필드          | 의미             |
| ----------- | -------------- |
| `cik`       | 토큰이 속한 법인의 CIK |
| `kid`       | Key ID         |
| `alg`       | 암호화 알고리즘       |
| `expiresAt` | 만료 시점          |

> 애플리케이션은 `expiresAt`을 통해 **API 호출 없이 만료 여부 사전 판단 가능**
> (단, 수동 revoke 여부는 확인 불가)

### 3.2 User API Token Header 예시

```json
{
  "kid": "75c8d379-2d82-415e-a3c5-7501699e3101",
  "alg": "ECDH-ES",
  "userId": "0ebc7681-2b96-42a8-8e76-efd8c23b1b13",
  "expiresAt": "2024-08-24T03:00:00Z"
}
```

---

## 4. 토큰 유효기간 (Token Expiration)

EDGAR 운영 중단을 방지하기 위해 **평일·업무시간 외 만료 회피 로직**이 적용됩니다.

| 토큰 종류           | 최소 유효기간 |
| --------------- | ------- |
| Filer API Token | 1년      |
| User API Token  | 30일     |

> 공휴일은 발급 시점에 미확정인 경우 반영되지 않을 수 있음

---

## 5. 인증 방식 (Authentication)

### 5.1 Authorization 헤더

```http
Authorization: bearer {token}
```

두 개의 토큰을 함께 사용할 경우:

```http
Authorization: bearer filerToken,userToken
```

* 허용 구분자: `,` 또는 공백
* API는 **CIK + 사용자 권한**을 기준으로 접근 허용 여부 판단

---

## 6. 인증 오류 (Authentication Errors)

인증 실패 시 **401 / 403** 응답과 함께 메시지가 반환됩니다.
토큰은 **index(1부터 시작)** 로 식별됩니다.

### 6.1 주요 오류 메시지

| 메시지                             | 의미          |
| ------------------------------- | ----------- |
| token is not in expected format | JWT 형식 오류   |
| missing required header field   | 헤더 변조       |
| token not valid for application | 잘못된 환경/시스템  |
| token expired or revoked        | 만료 또는 취소    |
| duplicate token type            | 동일 유형 토큰 중복 |

---

## 7. 통신 구조 (Communication)

### 7.1 예시: 시스템 상태 조회

```http
GET /status HTTP/1.1
Authorization: bearer {token}
Accept: application/json
```

### 7.2 응답 예시

```json
{
  "tracking": "5fdac24eb9160787519516fde4499652",
  "locator": "1fecde",
  "message": "EDGAR is operating normally.",
  "condition": "ACCEPTING"
}
```

#### 응답 공통 필드

| 필드         | 설명           |
| ---------- | ------------ |
| `tracking` | 헬프데스크 문의용 ID |
| `locator`  | 내부 추적 코드     |
| `messages` | 오류·알림 메시지    |

---

## 8. 요청 헤더 (Request Headers)

필수 헤더:

* `Authorization`

선택 헤더:

* `Expect: 100-continue`
  → 대용량 제출 시 사전 검증 가능

---

## 9. Rate Limiting

* 과도한 요청 시 **429 Too Many Requests**
* 제한 기준은 변경 가능

---

## 10. HTTP 응답 코드 요약

### 10.1 성공 / 일반 오류

| 코드  | 의미                |
| --- | ----------------- |
| 200 | 성공                |
| 202 | 제출 수락 (처리 진행 중)   |
| 204 | 데이터 없음            |
| 400 | 잘못된 요청            |
| 401 | 인증 실패             |
| 403 | 권한 없음             |
| 405 | 메서드 오류            |
| 406 | Accept 헤더 오류      |
| 411 | Content-Length 누락 |
| 413 | 용량 초과             |
| 415 | Media Type 오류     |
| 429 | 요청 과다             |
| 500 | 서버 오류             |
| 503 | 서비스 중단            |

---

## 11. User-Agent 요구사항

모든 요청에는 다음 헤더가 **강력히 권장**됩니다.

```http
User-Agent: VendorName/Version
```

* 미제공 시 400 오류 또는 NOTICE 메시지 가능

---

## 12. 서버 엔드포인트 (Server Endpoints)

| API                          | Base URL                                                           |
| ---------------------------- | ------------------------------------------------------------------ |
| EDGAR Operational Status API | [https://api.edgarfiling.sec.gov](https://api.edgarfiling.sec.gov) |
| EDGAR Submission API         | [https://api.edgarfiling.sec.gov](https://api.edgarfiling.sec.gov) |
| EDGAR Filer Management API   | [https://api.edgarfiling.sec.gov](https://api.edgarfiling.sec.gov) |

### 예시

```text
GET https://api.edgarfiling.sec.gov/status
```

---

## 13. Filer Management API

| 메서드    | 경로                    | 기능            |
| ------ | --------------------- | ------------- |
| POST   | /fm/enrollment        | EDGAR Next 등록 |
| GET    | /fm/{cik}             | 계정 정보 조회      |
| POST   | /fm/{cik}/ccc         | CCC 생성        |
| PUT    | /fm/{cik}/ccc         | CCC 커스터마이즈    |
| GET    | /fm/{cik}/delegations | 위임 조회         |
| POST   | /fm/{cik}/individuals | 개인 추가         |
| PUT    | /fm/{cik}/individuals | 역할 변경         |
| DELETE | /fm/{cik}/individuals | 개인 삭제         |

---

## 14. Submission API

* **Filer API Token + User API Token 필수**
* SINGLE / BULK 제출 방식 지원

### 14.1 제출 방식

| 방식     | 설명           |
| ------ | ------------ |
| SINGLE | 단일 제출        |
| BULK   | 복수 제출 (1 문서) |

### 주요 엔드포인트

```text
POST /submission/single/live
POST /submission/single/test
POST /submission/bulk/live
POST /submission/bulk/test
```

* 성공 시 **Accession Number** 반환
* 오류 여부는 Submission Status API에서 확인

---

## 15. Submission Status API

* 제출 상태 및 오류 코드 조회
* **Filer API Token + Accession Number** 필요

```text
POST /submission/status
GET  /submission/{accessionNumber}/status
```

### 상태 보관 기간

| 상태        | 보관 기간 |
| --------- | ----- |
| TEST 승인   | 2일    |
| LIVE 승인   | 60일   |
| SUSPENDED | 6일    |

---

### ✔️ 정리 포인트

* **JWT Header만으로도 만료 판단 가능**
* 실제 승인 여부는 **Status API 필수 확인**
* 제출 성공 ≠ EDGAR 최종 승인
* 위임(Delegation) 구조 이해가 핵심

---


# Overview of EDGAR APIs

**EDGAR Business Office, U.S. Securities and Exchange Commission**
**September 27, 2024** 

> 본 문서는 EDGAR Business Office 직원의 견해를 정리한 자료로, 규정/법령/위원회(SEC)의 공식 입장이 아니며 법적 효력은 없습니다. 

---

## Table of Contents 

1. Introduction to EDGAR APIs
2. Submission API
3. Submission Status API
4. EDGAR Operational Status API
5. Filer Management APIs

   * Add Individuals
   * Remove Individuals
   * View Individuals
   * Change Roles
   * Send Delegation Invitations
   * Request Delegation Invitations
   * View Delegations
   * View Filer Account Information
   * Generate CCC
   * Create Custom CCC
   * Filing Credentials Verification
   * Enrollment 

---

## I. Introduction to EDGAR APIs

### 1) EDGAR Next와 API 연결

* EDGAR Next의 일부로, EDGAR는 **API(기계-대-기계 통신 인터페이스)**를 제공합니다.
* **API 연결은 선택사항(Optional)**입니다. 

### 2) 연결 시 준수해야 할 기본 요건(요지)

* API를 사용하려면 **Regulation S-T Rule 10** 및 **EDGAR Filer Manual** 요구사항을 준수해야 합니다.
* **최소 2명의 Technical Administrator 승인(권한 부여)** 및 **Filer API Token 제시**가 요구됩니다(또는 위임된(delegated) 엔티티의 API 연결을 사용하는 경우 해당 엔티티가 위 요건을 준수해야 함). 

### 3) 토큰 요구사항(큰 원칙)

* **Submission Status API**, **EDGAR Operational Status API**를 제외한 대부분 API는 **User API Token**이 필요합니다.
* 특정 API는 **User API Token을 제시한 개인이 “Account Administrator”여야** 합니다(문서 각 API 항목에서 표시). 

### 4) 샘플 코드 및 참고자료

* SEC는 필러의 API 연결을 돕기 위한 **오픈소스 샘플(커스텀 filing application 예시)**을 제공합니다.
* “EDGAR API Development Toolkit(API Toolkit)”을 SEC.gov에서 참고하도록 안내합니다. 

---

## A. List of EDGAR APIs 

연결 가능한 API 목록:

1. Submission
2. Submission status
3. EDGAR operational status
4. Add individuals
5. Remove individuals
6. View individuals
7. Change roles
8. Send delegation invitations
9. Request delegation invitations
10. View delegations
11. View filer account information
12. Generate CCC
13. Create custom CCC
14. Verify filing credentials
15. Enrollment 

---

## B. General Workflow (공통 절차)

모든 API는 입력/출력은 달라도, 전반적 흐름은 동일하다고 설명합니다. 

1. (브라우저) Technical Admin이 EDGAR Filer Management 로그인 + MFA 
2. (필요 시) Technical Admin이 **Filer API Token** 생성 후 커스텀 앱에 안전하게 제공

   * 토큰은 **행위 대상 CIK**에서 발급되거나, 또는 **위임(delegation)된 CIK**에서 발급되어야 함 
3. 커스텀 filing application에 개인(관리자/사용자) 로그인 
4. (브라우저) 개인이 EDGAR 대시보드 로그인 + MFA 
5. (필요 시) 개인이 **User API Token** 생성 후 커스텀 앱에 안전하게 제공

   * Status/Operational Status 제외 대부분 API에서 필요 
6. 커스텀 앱이 토큰(필요 시 2종) + 입력값을 API로 전송 
7. API가 성공/실패 및 오류를 응답 
8. 커스텀 앱이 결과를 filer에게 안내 

> 대부분의 경우(유효 토큰이 이미 존재하면) **3, 6, 7, 8 단계만 수행**하면 된다고 설명합니다. 

---

## II. Submission API

* EDGAR에 **filing을 제출(submit)**하기 위한 API
* 입력: **Filer API Token + User API Token + 제출 데이터(body)**
* 옵션: **Login CIK**(특정 delegation 관계가 있을 때) 
* 처리 결과: 제출을 **process/validate**하고 각 요청에 대해 **accession number**를 발급 

> (도식) **Figure A**는 Submission API 워크플로우를, **Figure B**는 입력/출력 개요를 보여줍니다(각각 p.5~6). 

---

## III. Submission Status API

* 하나 이상 제출물의 **상태(status)**를 제공
* 입력: **Filer API Token + accession number(s)**
* 조건: accession number가 존재하고, **Filer API Token이 제출 주체 또는 해당 제출의 “filer” CIK**에 해당해야 함 
* 단일/복수 조회 도식:

  * Figure C: 단일
  * Figure D: 복수(리스트로 accession number 전달) 

---

## IV. EDGAR Operational Status API

* EDGAR의 **운영 상태(operational status)**를 제공
* 입력: **Filer API Token** 
* 반환 가능한 상태 예시: 

  * **Accepting submissions**: 운영시간 내 정상 접수
  * **Accepting submissions after hours**: 운영은 정상이나, 특정 서식만 “오늘 날짜”로 접수되고 나머지는 다음 영업일 처리
  * **Outside hours of operation**: 운영시간 외
  * **No communication**: 기술적 문제 등으로 API 통신 불가
* 입력/출력 도식: Figure E 

---

## V. Filer Management APIs (12개)

EDGAR Next API 설계에는 아래 12개 **Filer Management API**가 포함됩니다. 

1. Add individuals
2. Remove individuals
3. View individuals
4. Change roles
5. Send delegation invitations
6. Request delegation invitations
7. View delegations
8. View filer account information
9. Generate CCC
10. Create custom CCC
11. Verify filing credentials
12. Enrollment 

---

### A. Add Individuals API

**목적:** 특정 CIK(EDGAR account)에 개인을 역할(role)과 함께 추가 

**입력(요지):**

* Filer API Token
* User API Token
* 대상 CIK
* 개인 리스트(이름 + Login.gov 이메일)
* 부여할 역할 

**토큰/권한 요건:**

* User API Token의 개인은 **해당 CIK의 Account Administrator**
* Filer API Token은 **대상 CIK와 일치**하거나 **해당 CIK로부터 delegation**을 받아야 함 

**성공 시:** 성공 메시지 + 역할 초대(role invitations) 발송 

---

### B. Remove Individuals API

**목적:** 개인(1명 이상) 제거 

**입력(요지):**

* Filer API Token
* User API Token
* 대상 CIK
* 제거할 개인 리스트(Login.gov 이메일)
* 제거할 역할 

**요건(요지):**

* User API Token 개인: 해당 CIK의 Account Administrator
* Filer API Token: 대상 CIK 일치 또는 delegation 

---

### C. View Individuals API

**목적:** CIK에 역할을 가진 모든 개인 조회 

**입력:** Filer API Token + User API Token + 대상 CIK 
**요건:** User API Token 개인은 Account Administrator, Filer API Token은 일치 또는 delegation 
**성공 시:** role title, name, email 등을 반환 

---

### D. Change Roles API

**목적:** CIK 내 개인들의 역할 변경 

**입력(요지):**

* Filer API Token
* User API Token
* 대상 CIK
* 개인 리스트(Login.gov 이메일)
* 새 역할(업데이트된 roles) 

**요건(요지):**

* User API Token 개인: Account Administrator
* Filer API Token: 대상 CIK 일치 또는 delegation 

---

### E. Send Delegation Invitations API

**목적:** filer가 다른 EDGAR accounts/CIKs에 delegation 초대 발송
(수락되면 delegated entity가 되어 대신 제출 가능) 

**입력:** Filer API Token + User API Token + delegating filer CIK + delegated entity CIK 리스트 
**요건:**

* User API Token 개인: 해당 CIK의 Account Administrator
* Filer API Token: **반드시 그 CIK와 일치** 

---

### F. Request Delegation Invitations API

**목적:** (예: filing agent) EDGAR account가 filer들에게 delegation을 “요청”

* filer가 요청을 수락하면 invitation 발송 → 요청자가 수락하면 delegated entity가 됨 

**입력:** Filer API Token + User API Token + 요청자 CIK + 요청 받을 filer CIK(들) 
**요건:** User API Token 개인이 Account Administrator이고, Filer API Token은 요청자 CIK와 일치 

---

### G. View Delegations API

**목적:** CIK 기준으로 delegation 관계 목록 조회(Active/Pending/Requested/Deactivated) 

**입력:** Filer API Token + User API Token + 대상 CIK 
**요건:** User는 user 또는 account administrator 가능, Filer API Token은 대상 CIK와 일치 

---

### H. View Filer Account Information API

**목적:** CIK의 filer 정보 조회(회사명/주소/연간 확인일/CIK type/CCC 등) 

**입력:** Filer API Token + User API Token + 대상 CIK 
**요건:** user/account admin/delegated user/delegated account admin 가능, Filer API Token은 일치 또는 delegation 

---

### I. Generate CCC API

**목적:** 새 CCC를 무작위 생성 
**입력:** Filer API Token + User API Token + 대상 CIK 
**요건:** User는 Account Administrator, Filer API Token은 일치 또는 delegation 

---

### J. Create Custom CCC API

**목적:** CCC를 사용자가 지정한 값으로 변경(현재 CCC + 새 CCC 필요) 
**입력:** Filer API Token + User API Token + 대상 CIK + current CCC + new CCC 
**요건:** User는 Account Administrator, Filer API Token은 일치 또는 delegation 

---

### K. Filing Credentials Verification API

**목적:** 해당 토큰들로 filing 가능한지(검증) 및 만료일 등 정보 제공 

* GET 메서드: 헤더에 Filer/User token, URL path에 CIK 
* `valid = true`가 나오기 위한 요건:

  * User: user/account admin/delegated user/delegated account admin
  * Filer API Token: 일치 또는 delegation 
* 성공 시 제공 정보(예시):

  * filer token 만료일
  * user token 만료일
  * 연간 확인(annual confirmation) 예정일 

---

### L. Enrollment API

**목적:** 기존 filers를 EDGAR Next에 등록하고 account administrators 지정 

**입력(요지):**

* Filer API Token
* User API Token
* CIK
* current CCC
* passphrase
* account administrators 정보(이름/이메일 등) 

**특이점:**
Enrollment API에서의 Filer API Token은, 등록 대상 CIK들과 연계될 필요가 없을 수 있음(대상 계정들이 아직 대시보드 접근 전이라 token 발급 불가한 상황 고려). 

---


# Create and Manage Filer and User API Tokens

**(EDGAR Next – Filer Support Resources)**
*Last Reviewed / Updated: April 3, 2025*

---

## 1. 개요 (Introduction to API Tokens)

**API Token은 EDGAR Next API 사용을 위한 필수 보안 요건**입니다.

EDGAR API에는 두 가지 토큰이 존재합니다.

* **Filer API Token**
* **User API Token**

> 관련 배경 및 전체 구조는 다음 문서를 함께 참조하도록 안내됩니다.
>
> * *How Do I Understand EDGAR APIs*
> * *Overview of EDGAR APIs*
> * *API Development Toolkit*

---

## 2. Filer API Token

### 2.1 기본 원칙

* **모든 API 연결에는 Filer API Token이 필요**
* **유효기간: 1년**
* **Technical Administrator만 생성 가능**
* **각 filer는 최소 2명의 Technical Administrator 필요**
* Technical Administrator는 filer의 API 연결 전반을 관리함

### 2.2 토큰 수량 및 운용

* 생성 가능한 토큰 수: **제한 없음**
* 여러 개의 Filer API Token을 **동시에 병행 사용 가능**
* 토큰 간 **유효기간 중첩 허용**

---

### 2.3 위임 엔티티(Delegated Entity) 토큰 사용 예외

다음 조건을 충족하면 **위임된 엔티티의 Filer API Token 사용 가능**

* 위임 엔티티가:

  * 최소 2명의 Technical Administrator를 보유
  * API 관련 모든 요건 충족
* 단,

  * **User API Token은 반드시 “본인” 토큰을 사용**
  * (해당 API가 User API Token을 요구하는 경우)

---

## 3. Filer API Token 생성 절차

> **주체:** Technical Administrator

### 단계별 절차

1. Login.gov 개인 계정으로 **EDGAR Filer Management 로그인**
2. 대시보드에서 **My Accounts** 선택

   * 회색(비활성)일 경우: 해당 filer에 대한 권한 없음
3. 대상 filer 선택
4. **Manage Filer API Token** 선택

   * 회색일 경우: Technical Administrator 아님
5. **Create New Filer API Token** 선택
6. (선택) 토큰 이름 지정
7. **Create** 클릭

   * Technical Administrator가 2명 미만이면 오류 발생
8. 토큰 **복사 또는 다운로드**
9. **Done** 선택 → 생성 완료

### 생성 후 상태

* 토큰은 즉시 **Active**
* 만료일이 대시보드에 표시됨
* Login.gov 등록 이메일로 확인 메일 발송

---

## 4. Filer API Token 비활성화(Inactivate)

> **주체:** Technical Administrator

### 절차

1. Filer API Token 생성 절차의 1~4단계 수행
2. 대상 토큰의 **Actions(⋯)** 선택
3. **Inactivate → Yes, Inactivate**
4. 성공 메시지 표시
5. 토큰 상태가 **Inactive**로 변경

> 비활성화된 토큰은 **즉시 사용 불가**

---

## 5. User API Token

### 5.1 기본 원칙

* **개인 단위 토큰**
* **유효기간: 30일**
* **Account Administrator 또는 User가 직접 생성**
* Technical Administrator는 **User 역할이 아닌 한 생성 불가**

### 5.2 어떤 API에 필요한가

| API                    | User API Token 필요 여부 |
| ---------------------- | -------------------- |
| Submission API         | 필요                   |
| Filer Management API   | 필요                   |
| Submission Status API  | 불필요                  |
| Operational Status API | 불필요                  |

---

### 5.3 운용 제한 및 실무 포인트

* **개인당 1개의 User API Token만 활성 가능**
* 새 토큰 생성 시 **기존 토큰은 자동 비활성화**
* 마감 리스크 회피를 위해:

  * 여러 사용자/관리자가
  * **서로 다른 만료일의 User API Token**을 보유하도록 권장

---

## 6. User API Token 생성 절차

> **주체:** Account Administrator 또는 User

### 단계별 절차

1. Login.gov 개인 계정으로 **EDGAR Filer Management 로그인**
2. **My User API Token** 선택
3. 최초 생성 시 안내 메시지 확인
4. **Create User API Token** 선택
5. 안내문 검토 후 다시 **Create User API Token** 선택
6. 토큰 **복사 또는 다운로드**

### 토큰 갱신(비활성화) 방식

* **새 User API Token 생성 = 기존 토큰 자동 비활성화**
* 별도의 “Inactivate” 메뉴 없음

---

## 7. 핵심 정리 (실무 기준)

### 토큰 역할 요약

| 구분    | Filer API Token         | User API Token       |
| ----- | ----------------------- | -------------------- |
| 단위    | 법인(CIK)                 | 개인                   |
| 생성 주체 | Technical Administrator | User / Account Admin |
| 유효기간  | 1년                      | 30일                  |
| 수량 제한 | 없음                      | 1개 활성                |
| 주 용도  | API 연결 자체               | 제출·관리 권한 증명          |

### 실무 체크포인트

* API 연결 실패 → **대부분 토큰 문제**
* Submission API → **항상 두 토큰 조합 확인**
* User API Token 만료는 **가장 흔한 장애 원인**
* 위임(delegation) 구조에서는

  * **Filer API Token은 위임 엔티티 사용 가능**
  * **User API Token은 반드시 본인 것 사용**

---
