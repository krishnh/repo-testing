package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"container-api-prp/internal/models"

	"cloud.google.com/go/bigquery"
	"golang.org/x/sync/singleflight"
	"google.golang.org/api/iterator"
)

// — Server

type CacheEntry struct {
	Policies  []models.Policy
	ExpiresAt time.Time
}

type Server struct {
	BQ           *bigquery.Client
	ProjectID    string
	DatasetID    string
	TableID      string
	cacheTTL     time.Duration
	maxCacheSize int

	// Full dataset cache (new strategy)
	fullDatasetCache     []models.Policy
	fullDatasetExpiresAt time.Time
	fullDatasetMu        sync.RWMutex

	// Legacy per-query cache (kept for backward compatibility)
	searchCache map[string]CacheEntry
	cacheMu     sync.RWMutex
	single      singleflight.Group
}

const defaultCacheTTL = 8 * time.Hour
const defaultMaxCacheSize = 1000

type policyJSONRow struct {
	PolicyJSON string `bigquery:"policy_json"`
}

type policyJSONPayload struct {
	PolicyID        string                  `json:"policy_id"`
	PolicyType      string                  `json:"policy_type"`
	PolicyVersion   json.RawMessage         `json:"policy_version"`
	PolicyStatus    string                  `json:"policy_status"`
	SourceSystem    string                  `json:"source_system"`
	SourceReference string                  `json:"source_reference"`
	CreatedAt       string                  `json:"created_at"`
	ValidFrom       string                  `json:"valid_from"`
	ValidTo         *string                 `json:"valid_to"`
	Provenance      models.PolicyProvenance `json:"provenance"`
	Subjects        []models.PolicySubject  `json:"subjects"`
	Provisions      []models.Provision      `json:"provisions"`
}

type bqCompatProvenanceRow struct {
	LegalBasis    string `bigquery:"legal_basis"`
	CreatedReason string `bigquery:"created_reason"`
	CreatedBy     string `bigquery:"created_by"`
}

type bqCompatResourceRow struct {
	OtherID         bigquery.NullString `bigquery:"other_id"`
	EncounterCSN    bigquery.NullString `bigquery:"encounter_csn"`
	PatientMRN      bigquery.NullString `bigquery:"patient_mrn"`
	Confidentiality bigquery.NullString `bigquery:"confidentiality"`
	RecordType      bigquery.NullString `bigquery:"record_type"`
}

type bqCompatTargetRow struct {
	Resource bqCompatResourceRow `bigquery:"resource"`
	Purpose  string              `bigquery:"purpose"`
}

type bqCompatObligationDetailsRow struct {
	JustificationRequired bool     `bigquery:"justification_required"`
	Fields                []string `bigquery:"fields"`
}

type bqCompatObligationRow struct {
	Details bqCompatObligationDetailsRow `bigquery:"details"`
	Type    string                       `bigquery:"type"`
}

type bqCompatBTGScopeRow struct {
	Purpose      bigquery.NullString `bigquery:"purpose"`
	EncounterCSN bigquery.NullString `bigquery:"encounter_csn"`
	PatientMRN   bigquery.NullString `bigquery:"patient_mrn"`
}

type bqCompatTimeConstraintsRow struct {
	DelaySeconds bigquery.NullInt64  `bigquery:"delay_seconds"`
	NotAfter     bigquery.NullString `bigquery:"not_after"`
	ReleaseBasis bigquery.NullString `bigquery:"release_basis"`
	NotBefore    bigquery.NullString `bigquery:"not_before"`
}

type bqCompatConditionsRow struct {
	TimeConstraints bqCompatTimeConstraintsRow `bigquery:"time_constraints"`
	BTGScope        bqCompatBTGScopeRow        `bigquery:"btg_scope"`
	RequiresBTG     bool                       `bigquery:"requires_btg"`
}

type bqCompatProvisionRow struct {
	Conditions  bqCompatConditionsRow   `bigquery:"conditions"`
	Target      bqCompatTargetRow       `bigquery:"target"`
	Effect      string                  `bigquery:"effect"`
	Obligations []bqCompatObligationRow `bigquery:"obligations"`
	ProvisionID string                  `bigquery:"provision_id"`
}

type bqCompatPolicyRow struct {
	Provenance      bqCompatProvenanceRow  `bigquery:"provenance"`
	ValidTo         bigquery.NullString    `bigquery:"valid_to"`
	ValidFrom       time.Time              `bigquery:"valid_from"`
	CreatedAt       time.Time              `bigquery:"created_at"`
	Subjects        []models.BQSubjectRow  `bigquery:"subjects"`
	SourceReference string                 `bigquery:"source_reference"`
	SourceSystem    string                 `bigquery:"source_system"`
	PolicyStatus    string                 `bigquery:"policy_status"`
	Provisions      []bqCompatProvisionRow `bigquery:"provisions"`
	PolicyVersion   bigquery.NullFloat64   `bigquery:"policy_version"`
	PolicyType      string                 `bigquery:"policy_type"`
	PolicyID        string                 `bigquery:"policy_id"`
}

func NewServer(projectID, datasetID, tableID string, _ bool) *Server {
	// Read cache TTL from environment variable
	cacheTTL := defaultCacheTTL
	if ttlStr := os.Getenv("CACHE_TTL"); ttlStr != "" {
		if parsedTTL, err := time.ParseDuration(ttlStr); err == nil && parsedTTL > 0 {
			cacheTTL = parsedTTL
		}
	}

	// Read max cache size from environment variable
	maxCacheSize := defaultMaxCacheSize
	if sizeStr := os.Getenv("CACHE_MAX_SIZE"); sizeStr != "" {
		if parsedSize, err := strconv.Atoi(sizeStr); err == nil && parsedSize > 0 {
			maxCacheSize = parsedSize
		}
	}

	return &Server{
		ProjectID:    projectID,
		DatasetID:    datasetID,
		TableID:      tableID,
		cacheTTL:     cacheTTL,
		maxCacheSize: maxCacheSize,
		searchCache:  make(map[string]CacheEntry),
	}
}

func (s *Server) SetCacheTTL(ttl time.Duration) {
	if ttl <= 0 {
		s.cacheTTL = defaultCacheTTL
		return
	}
	s.cacheTTL = ttl
}

// buildCacheKey creates a cache key from search request parameters using pipe symbol
func buildCacheKey(req models.SearchRequest) string {
	var parts []string
	parts = append(parts, req.Purpose)
	if req.Actor != nil {
		if req.Actor.ID != nil {
			parts = append(parts, "id:"+*req.Actor.ID)
		}
		if req.Actor.IDType != nil {
			parts = append(parts, "id_type:"+*req.Actor.IDType)
		}
		if req.Actor.Role != nil {
			parts = append(parts, "role:"+*req.Actor.Role)
		}
	}
	return strings.Join(parts, "|")
}

// getFullDatasetCache retrieves the full dataset cache if it exists and hasn't expired
func (s *Server) getFullDatasetCache(now time.Time) ([]models.Policy, bool) {
	s.fullDatasetMu.RLock()
	policies := s.fullDatasetCache
	expiresAt := s.fullDatasetExpiresAt
	s.fullDatasetMu.RUnlock()

	if policies == nil || now.After(expiresAt) {
		return nil, false
	}

	// Return a copy to avoid race conditions
	results := make([]models.Policy, len(policies))
	copy(results, policies)
	return results, true
}

// setFullDatasetCache stores the full dataset in cache
func (s *Server) setFullDatasetCache(policies []models.Policy, now time.Time) {
	// Store a copy to avoid race conditions
	cached := make([]models.Policy, len(policies))
	copy(cached, policies)

	s.fullDatasetMu.Lock()
	s.fullDatasetCache = cached
	s.fullDatasetExpiresAt = now.Add(s.cacheTTL)
	s.fullDatasetMu.Unlock()
}

// loadFullDataset loads all policies from BigQuery and caches them
func (s *Server) loadFullDataset(ctx context.Context) ([]models.Policy, error) {
	now := time.Now().UTC()

	// Check if we have a valid cached full dataset
	if cached, ok := s.getFullDatasetCache(now); ok {
		return cached, nil
	}

	// Use singleflight to prevent duplicate BigQuery calls
	result, err, _ := s.single.Do("full_dataset", func() (interface{}, error) {
		// Double-check cache inside singleflight
		if cached, ok := s.getFullDatasetCache(time.Now().UTC()); ok {
			return cached, nil
		}

		// Query BigQuery for all policies
		policies, err := s.queryCurrentPolicies(ctx)
		if err != nil {
			return nil, err
		}

		// Cache the full dataset
		s.setFullDatasetCache(policies, time.Now().UTC())

		return policies, nil
	})

	if err != nil {
		return nil, err
	}

	return result.([]models.Policy), nil
}

// getSearchCache retrieves a cache entry if it exists and hasn't expired (legacy per-query cache)
func (s *Server) getSearchCache(key string, now time.Time) ([]models.Policy, bool) {
	s.cacheMu.RLock()
	entry, ok := s.searchCache[key]
	s.cacheMu.RUnlock()

	if !ok {
		return nil, false
	}

	if now.After(entry.ExpiresAt) {
		s.cacheMu.Lock()
		delete(s.searchCache, key)
		s.cacheMu.Unlock()
		return nil, false
	}

	// Return a copy to avoid race conditions
	results := make([]models.Policy, len(entry.Policies))
	copy(results, entry.Policies)
	return results, true
}

// setSearchCache stores a cache entry with eviction if at capacity (legacy per-query cache)
func (s *Server) setSearchCache(key string, policies []models.Policy, now time.Time) {
	// Evict oldest entry if at capacity
	if len(s.searchCache) >= s.maxCacheSize {
		s.evictOldestEntry(now)
	}

	// Store a copy to avoid race conditions
	cached := make([]models.Policy, len(policies))
	copy(cached, policies)

	s.cacheMu.Lock()
	s.searchCache[key] = CacheEntry{
		Policies:  cached,
		ExpiresAt: now.Add(s.cacheTTL),
	}
	s.cacheMu.Unlock()
}

// evictOldestEntry removes the oldest entry by expiration time
func (s *Server) evictOldestEntry(now time.Time) {
	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, entry := range s.searchCache {
		if first || entry.ExpiresAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.ExpiresAt
			first = false
		}
	}

	if oldestKey != "" {
		delete(s.searchCache, oldestKey)
	}
}

// — BQ conversion helpers

func toNullString(s *string) bigquery.NullString {
	if s == nil {
		return bigquery.NullString{}
	}
	return bigquery.NullString{StringVal: *s, Valid: true}
}

func fromNullString(ns bigquery.NullString) *string {
	if !ns.Valid {
		return nil
	}
	s := ns.StringVal
	return &s
}

func PolicyToBQRow(p models.Policy, updateType string) models.BQPolicyRow {
	subjects := make([]models.BQSubjectRow, len(p.Subjects))
	for i, subj := range p.Subjects { //nolint
		subjects[i] = models.BQSubjectRow{
			SubjectID:   subj.SubjectID,
			SubjectType: subj.SubjectType,
			Actor: models.BQActorRow{
				ID:           toNullString(subj.Actor.ID),
				Role:         toNullString(subj.Actor.Role),
				IdentityType: toNullString(subj.Actor.IdentityType),
			},
		}
	}

	provisions := make([]models.BQProvisionRow, len(p.Provisions))
	for i, prov := range p.Provisions {
		cond := models.BQConditionsRow{
			RequiresBTG:            prov.Conditions.RequiresBTG,
			BTGScopePatientMRN:     toNullString(prov.Conditions.BTGScope.PatientMRN),
			BTGScopeEncounterCSN:   toNullString(prov.Conditions.BTGScope.EncounterCSN),
			BTGScopePurpose:        toNullString(prov.Conditions.BTGScope.Purpose),
			ConditionReleaseBasis:  toNullString(prov.Conditions.TimeConstraints.ReleaseBasis),
			ObligationRedactFields: []string{},
		}
		if prov.Conditions.TimeConstraints.NotBefore != nil {
			cond.ConditionNotBefore = bigquery.NullTimestamp{Timestamp: *prov.Conditions.TimeConstraints.NotBefore, Valid: true}
		}
		if prov.Conditions.TimeConstraints.NotAfter != nil {
			cond.ConditionNotAfter = bigquery.NullTimestamp{Timestamp: *prov.Conditions.TimeConstraints.NotAfter, Valid: true}
		}
		if prov.Conditions.TimeConstraints.DelaySeconds != nil {
			cond.ConditionDelaySeconds = bigquery.NullInt64{Int64: int64(*prov.Conditions.TimeConstraints.DelaySeconds), Valid: true}
		}

		for _, obl := range prov.Obligations {
			switch obl.Type {
			case "BTG_REQUIRED":
				cond.ObligationBTGRequired = bigquery.NullBool{Bool: true, Valid: true}
				cond.ObligationJustificationRequired = bigquery.NullBool{Bool: obl.Details.JustificationRequired, Valid: true}
			case "REDACT":
				cond.ObligationRedact = bigquery.NullBool{Bool: true, Valid: true}
				if len(obl.Details.Fields) > 0 {
					cond.ObligationRedactFields = obl.Details.Fields
				}
			}
		}

		provisions[i] = models.BQProvisionRow{
			ProvisionID: prov.ProvisionID,
			Effect:      prov.Effect,
			Target: models.BQTargetRow{
				Purpose: prov.Target.Purpose,
				Resource: models.BQResourceRow{
					RecordType:      toNullString(prov.Target.Resource.RecordType),
					Confidentiality: toNullString(prov.Target.Resource.Confidentiality),
					PatientMRN:      toNullString(prov.Target.Resource.PatientMRN),
					EncounterCSN:    toNullString(prov.Target.Resource.EncounterCSN),
					OtherID:         toNullString(prov.Target.Resource.OtherID),
				},
			},
			Conditions: cond,
		}
	}

	row := models.BQPolicyRow{
		PolicyID:                p.PolicyID,
		PolicyType:              p.PolicyType,
		PolicyVersion:           p.PolicyVersion,
		PolicyStatus:            p.PolicyStatus,
		SourceSystem:            p.SourceSystem,
		SourceReference:         p.SourceReference,
		CreatedAt:               p.CreatedAt,
		ValidFrom:               p.ValidFrom,
		ProvenanceCreatedBy:     p.Provenance.CreatedBy,
		ProvenanceCreatedReason: p.Provenance.CreatedReason,
		ProvenanceLegalBasis:    p.Provenance.LegalBasis,
		Subjects:                subjects,
		Provisions:              provisions,
		UpdateType:              updateType,
		RowInserted:             time.Now().UTC(),
	}

	if p.ValidTo != nil {
		row.ValidTo = bigquery.NullTimestamp{Timestamp: *p.ValidTo, Valid: true}
	}
	return row
}

func BQRowToPolicy(row models.BQPolicyRow) models.Policy {
	subjects := make([]models.PolicySubject, len(row.Subjects))
	for i, subj := range row.Subjects {
		subjects[i] = models.PolicySubject{
			SubjectID:   subj.SubjectID,
			SubjectType: subj.SubjectType,
			Actor: models.PolicyActor{
				ID:           fromNullString(subj.Actor.ID),
				Role:         fromNullString(subj.Actor.Role),
				IdentityType: fromNullString(subj.Actor.IdentityType),
			},
		}
	}

	provisions := make([]models.Provision, len(row.Provisions))
	for i, prov := range row.Provisions {
		cond := models.ProvisionConditions{
			RequiresBTG: prov.Conditions.RequiresBTG,
			BTGScope: models.BTGScope{
				PatientMRN:   fromNullString(prov.Conditions.BTGScopePatientMRN),
				EncounterCSN: fromNullString(prov.Conditions.BTGScopeEncounterCSN),
				Purpose:      fromNullString(prov.Conditions.BTGScopePurpose),
			},
			TimeConstraints: models.TimeConstraints{
				ReleaseBasis: fromNullString(prov.Conditions.ConditionReleaseBasis),
			},
		}

		if prov.Conditions.ConditionNotBefore.Valid {
			t := prov.Conditions.ConditionNotBefore.Timestamp
			cond.TimeConstraints.NotBefore = &t
		}
		if prov.Conditions.ConditionNotAfter.Valid {
			t := prov.Conditions.ConditionNotAfter.Timestamp
			cond.TimeConstraints.NotAfter = &t
		}
		if prov.Conditions.ConditionDelaySeconds.Valid {
			ds := int(prov.Conditions.ConditionDelaySeconds.Int64)
			cond.TimeConstraints.DelaySeconds = &ds
		}

		obligs := []models.Obligation{}
		if prov.Conditions.ObligationBTGRequired.Valid && prov.Conditions.ObligationBTGRequired.Bool {
			obligs = append(obligs, models.Obligation{
				Type: "BTG_REQUIRED",
				Details: models.ObligationDetails{
					Fields:                []string{},
					JustificationRequired: prov.Conditions.ObligationJustificationRequired.Valid && prov.Conditions.ObligationJustificationRequired.Bool,
				},
			})
		}
		if prov.Conditions.ObligationRedact.Valid && prov.Conditions.ObligationRedact.Bool {
			fields := prov.Conditions.ObligationRedactFields
			if fields == nil {
				fields = []string{}
			}
			obligs = append(obligs, models.Obligation{
				Type:    "REDACT",
				Details: models.ObligationDetails{Fields: fields},
			})
		}

		provisions[i] = models.Provision{
			ProvisionID: prov.ProvisionID,
			Effect:      prov.Effect,
			Target: models.ProvisionTarget{
				Purpose: prov.Target.Purpose,
				Resource: models.ProvisionResource{
					RecordType:      fromNullString(prov.Target.Resource.RecordType),
					Confidentiality: fromNullString(prov.Target.Resource.Confidentiality),
					PatientMRN:      fromNullString(prov.Target.Resource.PatientMRN),
					EncounterCSN:    fromNullString(prov.Target.Resource.EncounterCSN),
					OtherID:         fromNullString(prov.Target.Resource.OtherID),
				},
			},
			Conditions:  cond,
			Obligations: obligs,
		}
	}

	p := models.Policy{
		PolicyID:        row.PolicyID,
		PolicyType:      row.PolicyType,
		PolicyVersion:   row.PolicyVersion,
		PolicyStatus:    row.PolicyStatus,
		SourceSystem:    row.SourceSystem,
		SourceReference: row.SourceReference,
		CreatedAt:       row.CreatedAt,
		ValidFrom:       row.ValidFrom,
		Provenance: models.PolicyProvenance{
			CreatedBy:     row.ProvenanceCreatedBy,
			CreatedReason: row.ProvenanceCreatedReason,
			LegalBasis:    row.ProvenanceLegalBasis,
		},
		Subjects:   subjects,
		Provisions: provisions,
	}
	if row.ValidTo.Valid {
		t := row.ValidTo.Timestamp
		p.ValidTo = &t
	}
	return p
}

func parseBQTimestamp(v string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999 UTC",
		"2006-01-02 15:04:05 UTC",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format: %q", v)
}

func payloadToPolicy(payload policyJSONPayload) (models.Policy, error) {
	if payload.PolicyID == "" {
		return models.Policy{}, fmt.Errorf("policy_id is required")
	}

	createdAt, err := parseBQTimestamp(payload.CreatedAt)
	if err != nil {
		return models.Policy{}, fmt.Errorf("parse created_at: %w", err)
	}
	validFrom, err := parseBQTimestamp(payload.ValidFrom)
	if err != nil {
		return models.Policy{}, fmt.Errorf("parse valid_from: %w", err)
	}

	policyVersion := ""
	if len(payload.PolicyVersion) > 0 && string(payload.PolicyVersion) != "null" {
		if payload.PolicyVersion[0] == '"' {
			if err := json.Unmarshal(payload.PolicyVersion, &policyVersion); err != nil {
				return models.Policy{}, fmt.Errorf("parse policy_version string: %w", err)
			}
		} else {
			var num float64
			if err := json.Unmarshal(payload.PolicyVersion, &num); err != nil {
				return models.Policy{}, fmt.Errorf("parse policy_version number: %w", err)
			}
			policyVersion = strconv.FormatFloat(num, 'f', -1, 64)
		}
	}

	p := models.Policy{
		PolicyID:        payload.PolicyID,
		PolicyType:      payload.PolicyType,
		PolicyVersion:   policyVersion,
		PolicyStatus:    payload.PolicyStatus,
		SourceSystem:    payload.SourceSystem,
		SourceReference: payload.SourceReference,
		CreatedAt:       createdAt,
		ValidFrom:       validFrom,
		Provenance:      payload.Provenance,
		Subjects:        payload.Subjects,
		Provisions:      payload.Provisions,
	}

	if payload.ValidTo != nil && strings.TrimSpace(*payload.ValidTo) != "" {
		vt, err := parseBQTimestamp(*payload.ValidTo)
		if err != nil {
			return models.Policy{}, fmt.Errorf("parse valid_to: %w", err)
		}
		p.ValidTo = &vt
	}

	return p, nil
}

func toNullTimestampString(t *time.Time) bigquery.NullString {
	if t == nil {
		return bigquery.NullString{}
	}
	return bigquery.NullString{StringVal: t.UTC().Format(time.RFC3339Nano), Valid: true}
}

func policyToCompatBQRow(p models.Policy) (bqCompatPolicyRow, error) {
	subjects := make([]models.BQSubjectRow, len(p.Subjects))
	for i, subj := range p.Subjects {
		subjects[i] = models.BQSubjectRow{
			SubjectID:   subj.SubjectID,
			SubjectType: subj.SubjectType,
			Actor: models.BQActorRow{
				ID:           toNullString(subj.Actor.ID),
				Role:         toNullString(subj.Actor.Role),
				IdentityType: toNullString(subj.Actor.IdentityType),
			},
		}
	}

	provisions := make([]bqCompatProvisionRow, len(p.Provisions))
	for i, prov := range p.Provisions {
		obligations := make([]bqCompatObligationRow, len(prov.Obligations))
		for j, obl := range prov.Obligations {
			fields := obl.Details.Fields
			if fields == nil {
				fields = []string{}
			}
			obligations[j] = bqCompatObligationRow{
				Type: obl.Type,
				Details: bqCompatObligationDetailsRow{
					JustificationRequired: obl.Details.JustificationRequired,
					Fields:                fields,
				},
			}
		}

		tc := bqCompatTimeConstraintsRow{
			NotBefore:    toNullTimestampString(prov.Conditions.TimeConstraints.NotBefore),
			NotAfter:     toNullTimestampString(prov.Conditions.TimeConstraints.NotAfter),
			ReleaseBasis: toNullString(prov.Conditions.TimeConstraints.ReleaseBasis),
		}
		if prov.Conditions.TimeConstraints.DelaySeconds != nil {
			tc.DelaySeconds = bigquery.NullInt64{Int64: int64(*prov.Conditions.TimeConstraints.DelaySeconds), Valid: true}
		}

		provisions[i] = bqCompatProvisionRow{
			ProvisionID: prov.ProvisionID,
			Target: bqCompatTargetRow{
				Purpose: prov.Target.Purpose,
				Resource: bqCompatResourceRow{
					RecordType:      toNullString(prov.Target.Resource.RecordType),
					Confidentiality: toNullString(prov.Target.Resource.Confidentiality),
					PatientMRN:      toNullString(prov.Target.Resource.PatientMRN),
					EncounterCSN:    toNullString(prov.Target.Resource.EncounterCSN),
					OtherID:         toNullString(prov.Target.Resource.OtherID),
				},
			},
			Effect: prov.Effect,
			Conditions: bqCompatConditionsRow{
				RequiresBTG: prov.Conditions.RequiresBTG,
				BTGScope: bqCompatBTGScopeRow{
					PatientMRN:   toNullString(prov.Conditions.BTGScope.PatientMRN),
					EncounterCSN: toNullString(prov.Conditions.BTGScope.EncounterCSN),
					Purpose:      toNullString(prov.Conditions.BTGScope.Purpose),
				},
				TimeConstraints: tc,
			},
			Obligations: obligations,
		}
	}

	policyVersion := bigquery.NullFloat64{}
	if strings.TrimSpace(p.PolicyVersion) != "" {
		v, err := strconv.ParseFloat(p.PolicyVersion, 64)
		if err != nil {
			return bqCompatPolicyRow{}, fmt.Errorf("invalid policy_version %q: %w", p.PolicyVersion, err)
		}
		policyVersion = bigquery.NullFloat64{Float64: v, Valid: true}
	}

	row := bqCompatPolicyRow{
		PolicyID:        p.PolicyID,
		PolicyType:      p.PolicyType,
		PolicyVersion:   policyVersion,
		PolicyStatus:    p.PolicyStatus,
		SourceSystem:    p.SourceSystem,
		SourceReference: p.SourceReference,
		CreatedAt:       p.CreatedAt,
		ValidFrom:       p.ValidFrom,
		Provenance: bqCompatProvenanceRow{
			CreatedBy:     p.Provenance.CreatedBy,
			CreatedReason: p.Provenance.CreatedReason,
			LegalBasis:    p.Provenance.LegalBasis,
		},
		Subjects:   subjects,
		Provisions: provisions,
	}

	if p.ValidTo != nil {
		row.ValidTo = bigquery.NullString{StringVal: p.ValidTo.UTC().Format(time.RFC3339Nano), Valid: true}
	}

	return row, nil
}

func (s *Server) tableAuditColumns(ctx context.Context) (string, string, error) {
	md, err := s.BQ.DatasetInProject(s.ProjectID, s.DatasetID).Table(s.TableID).Metadata(ctx)
	if err != nil {
		return "", "", err
	}

	updateTypeCol := ""
	rowInsertedCol := ""
	for _, f := range md.Schema {
		switch {
		case strings.EqualFold(f.Name, "UpdateType"), strings.EqualFold(f.Name, "update_type"):
			updateTypeCol = f.Name
		case strings.EqualFold(f.Name, "RowInserted"), strings.EqualFold(f.Name, "row_inserted"):
			rowInsertedCol = f.Name
		}
	}
	return updateTypeCol, rowInsertedCol, nil
}

func (s *Server) insertPolicyRow(ctx context.Context, p models.Policy, updateType string) error {
	if s.BQ == nil || s.ProjectID == "" || s.DatasetID == "" || s.TableID == "" {
		return fmt.Errorf("bigquery is not configured")
	}

	ins := s.BQ.DatasetInProject(s.ProjectID, s.DatasetID).Table(s.TableID).Inserter()
	updateTypeCol, rowInsertedCol, err := s.tableAuditColumns(ctx)
	if err != nil {
		return fmt.Errorf("table metadata: %w", err)
	}

	if updateTypeCol != "" && rowInsertedCol != "" {
		bqRow := PolicyToBQRow(p, updateType)
		if err := ins.Put(ctx, bqRow); err != nil {
			return fmt.Errorf("insert audit row: %w", err)
		}
		return nil
	}

	compatRow, err := policyToCompatBQRow(p)
	if err != nil {
		return fmt.Errorf("build compat row: %w", err)
	}
	if err := ins.Put(ctx, &compatRow); err != nil {
		return fmt.Errorf("insert compat row: %w", err)
	}
	return nil
}

// — BigQuery read helpers —

func (s *Server) queryCurrentPolicies(ctx context.Context) ([]models.Policy, error) {
	if s.BQ == nil || s.ProjectID == "" || s.DatasetID == "" || s.TableID == "" {
		return nil, fmt.Errorf("bigquery is not configured")
	}

	updateTypeCol, rowInsertedCol, err := s.tableAuditColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("query policies metadata: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", s.ProjectID, s.DatasetID, s.TableID)
	compatMode := false
	if updateTypeCol != "" && rowInsertedCol != "" {
		query = fmt.Sprintf(
			"SELECT * EXCEPT(rn) FROM ("+
				"SELECT t.*, ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY %s DESC) AS rn "+
				"FROM `%s.%s.%s` t "+
				"WHERE %s != 'DELETE'"+
				") WHERE rn = 1",
			rowInsertedCol, s.ProjectID, s.DatasetID, s.TableID, updateTypeCol,
		)
	} else {
		compatMode = true
		query = fmt.Sprintf("SELECT TO_JSON_STRING(t) AS policy_json FROM `%s.%s.%s` t", s.ProjectID, s.DatasetID, s.TableID)
	}

	q := s.BQ.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query policies: %w", err)
	}

	policies := make([]models.Policy, 0)
	for {
		if compatMode {
			var row policyJSONRow
			if err := it.Next(&row); err == iterator.Done {
				break
			} else if err != nil {
				return nil, fmt.Errorf("query policies read: %w", err)
			}

			var payload policyJSONPayload
			if err := json.Unmarshal([]byte(row.PolicyJSON), &payload); err != nil {
				return nil, fmt.Errorf("query policies decode row json: %w", err)
			}
			p, err := payloadToPolicy(payload)
			if err != nil {
				return nil, fmt.Errorf("query policies convert row: %w", err)
			}
			policies = append(policies, p)
			continue
		}

		var row models.BQPolicyRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("query policies read: %w", err)
		}
		policies = append(policies, BQRowToPolicy(row))
	}

	return policies, nil
}

func (s *Server) queryPolicyByID(ctx context.Context, policyID string) (*models.Policy, error) {
	p, err := s.queryCurrentPolicies(ctx)
	if err != nil {
		return nil, err
	}
	for i := range p {
		if p[i].PolicyID == policyID {
			return &p[i], nil
		}
	}
	return nil, nil
}

// — Handlers

func (s *Server) HandleHealthcheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"mode":   "bigquery",
	})
}

func (s *Server) HandleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Purpose == "" {
		http.Error(w, "purpose is required", http.StatusBadRequest)
		return
	}
	if req.Actor == nil {
		http.Error(w, "actor is required", http.StatusBadRequest)
		return
	}

	hasID := req.Actor.ID != nil && *req.Actor.ID != ""
	hasIDType := req.Actor.IDType != nil && *req.Actor.IDType != ""
	hasRole := req.Actor.Role != nil && *req.Actor.Role != ""

	if hasID != hasIDType {
		http.Error(w, "actor.id and actor.id_type must be provided together", http.StatusBadRequest)
		return
	}
	if !hasRole && !hasID {
		http.Error(w, "either actor.role or actor.id and actor.id_type must be provided", http.StatusBadRequest)
		return
	}

	// Load full dataset from cache or BigQuery
	policies, err := s.loadFullDataset(r.Context())
	if err != nil {
		log.Printf("search query error: %v", err)
		http.Error(w, "failed to search policies", http.StatusInternalServerError)
		return
	}

	// Filter policies in-memory
	matches := make([]models.Policy, 0)
	for _, p := range policies {
		if p.PolicyStatus != "ACTIVE" {
			continue
		}
		if !policyMatchesPurpose(p, req.Purpose) {
			continue
		}
		if !policyMatchesActor(p, req.Actor, hasID, hasRole) {
			continue
		}
		matches = append(matches, p)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matches)
}

func policyMatchesPurpose(p models.Policy, purpose string) bool {
	for _, prov := range p.Provisions {
		if prov.Target.Purpose == purpose {
			return true
		}
	}
	return false
}

func policyMatchesActor(p models.Policy, actor *models.SearchActor, hasID bool, hasRole bool) bool {
	idMatch := false
	roleMatch := false

	for _, subj := range p.Subjects {
		if hasID && subj.Actor.ID != nil && subj.Actor.IdentityType != nil {
			if *subj.Actor.ID == *actor.ID && *subj.Actor.IdentityType == *actor.IDType {
				idMatch = true
			}
		}
		if hasRole && subj.Actor.Role != nil {
			if *subj.Actor.Role == *actor.Role {
				roleMatch = true
			}
		}
		if (hasID && idMatch) || (hasRole && roleMatch) {
			break
		}
	}

	if hasID && hasRole {
		return idMatch || roleMatch
	}
	if hasID {
		return idMatch
	}
	return roleMatch
}
