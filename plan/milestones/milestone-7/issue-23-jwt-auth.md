# Issue #23 — JWT Authentication on FastAPI

**Milestone**: 7 — Security + Integration Testing  
**Labels**: `security` `api` `authentication` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Implement JWT token-based authentication on all FastAPI endpoints with role-based access control.

## Roles

| Role | Access |
|---|---|
| `rider` | Read trip status + reserve trips (`GET /trips/*`, `POST /trips`) |
| `admin` | Full access (all endpoints including `/vehicles`, `/demand/forecast`) |

## Implementation

### Token Generation
- [ ] Endpoint: `POST /auth/token`
- [ ] Input: username + role
- [ ] Output: JWT token with role in payload
- [ ] Library: `python-jose`
- [ ] Token expiry: configurable (default: 24h)

### Route Protection
- [ ] Use FastAPI `Depends()` for JWT verification on each route
- [ ] Rider tokens cannot access:
  - `GET /api/v1/vehicles/zone/{zone_id}`
  - `POST /api/v1/demand/forecast`
- [ ] Admin tokens have full access
- [ ] Return 401 for missing/invalid tokens
- [ ] Return 403 for insufficient permissions

### Testing
- [ ] Test with `curl` commands for each role
- [ ] Verify rider cannot access admin endpoints
- [ ] Verify expired tokens are rejected
- [ ] Verify tampered tokens are rejected

## Acceptance Criteria

- [ ] `/auth/token` generates valid JWT tokens
- [ ] All endpoints require valid JWT
- [ ] Role-based access control enforced
- [ ] Tested with `curl` for both roles
