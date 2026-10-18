package main

import (
	"net/http"
	"reflect"
	"testing"
)

func TestAddMemberHandler_upsertPreservesDeclarationAndLegacyConflict(t *testing.T) {
	scrubPoolEnv(t)
	t.Setenv("AQG_POOL_AUTO_BASE_URL", "https://pool.example")
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "cred-a|https://member.example")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "cred-b")
	t.Setenv("AQG_POOL_AUTO_PRIORITY", "b,a")
	pools := loadPools(t)
	// Establish the disabled declaration before taking the persistence snapshot.
	if status, err := pools.SetMemberDisabled("auto", "a", true); status != http.StatusOK || err != nil {
		t.Fatalf("disable a: status=%d err=%v, want 200", status, err)
	}
	srv := configMux(t, pools)

	before := pools.CurrentRegistry().Spec()
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a",
		`{"credential":"cred-a","base_url":"https://drift.example","placement":["a","b"],"upsert":true}`,
		http.StatusOK)
	if after := pools.CurrentRegistry().Spec(); !reflect.DeepEqual(after, before) {
		t.Fatalf("same-credential handler upsert changed config:\nbefore=%+v\nafter=%+v", before, after)
	}

	// The explicit bit is the compatibility boundary: old callers still get
	// the historical duplicate refusal.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a", `{"credential":"cred-a"}`, http.StatusConflict)

	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a",
		`{"credential":"cred-a-rotated","base_url":"https://other.example","placement":["a","b"],"upsert":true}`,
		http.StatusOK)
	rotated := pools.CurrentRegistry().Spec()
	if rotated.Pools["auto"].Members["a"].Credential != "cred-a-rotated" {
		t.Fatalf("rotated credential=%q, want cred-a-rotated", rotated.Pools["auto"].Members["a"].Credential)
	}
	beforePool := before.Pools["auto"]
	beforeMember := beforePool.Members["a"]
	beforeMember.Credential = "cred-a-rotated"
	beforePool.Members["a"] = beforeMember
	before.Pools["auto"] = beforePool
	if !reflect.DeepEqual(rotated.Pools["auto"], before.Pools["auto"]) {
		t.Fatalf("rotation changed declaration fields:\nbefore=%+v\nafter=%+v", before.Pools["auto"], rotated.Pools["auto"])
	}

	collisionBefore := pools.CurrentRegistry().Spec()
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a",
		`{"credential":"cred-b","base_url":"https://bad.example","upsert":true}`,
		http.StatusBadRequest)
	if after := pools.CurrentRegistry().Spec(); !reflect.DeepEqual(after, collisionBefore) {
		t.Fatalf("credential collision changed config:\nbefore=%+v\nafter=%+v", collisionBefore, after)
	}
}
