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
			tc.DelaySeconds = bigquery.NullInt64{
				Int64: int64(*prov.Conditions.TimeConstraints.DelaySeconds),
				Valid: true,
			}
		}

		provisions[i] = bqCompatProvisionRow{
			ProvisionID: prov.ProvisionID,
			Effect:      prov.Effect,
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
			Conditions: bqCompatConditionsRow{
				RequiresBTG: prov.Conditions.RequiredBTG,
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
		Provenance: bqCompatProvenanceRow{
			CreatedBy:     p.Provenance.CreatedBy,
			CreatedReason: p.Provenance.CreatedReason,
			LegalBasis:    p.Provenance.LegalBasis,
		},
		ValidFrom:       p.ValidFrom,
		CreatedAt:       p.CreatedAt,
		Subjects:        subjects,
		SourceReference: p.SourceReference,
		SourceSystem:    p.SourceSystem,
		PolicyStatus:    p.PolicyStatus,
		Provisions:      provisions,
		PolicyVersion:   policyVersion,
		PolicyType:      p.PolicyType,
		PolicyID:        p.PolicyID,
	}

	if p.ValidTo != nil {
		row.ValidTo = bigquery.NullString{
			StringVal: p.ValidTo.UTC().Format(time.RFC3339Nano),
			Valid:     true,
		}
	}

	return row, nil
}

func (s *Server) tableAuditColumns(ctx context.Context) (string, string, error) {
	if s.BQ == nil || s.ProjectID == "" || s.DatasetID == "" || s.TableID == "" {
		return "", "", fmt.Errorf("bigquery is not configured")
	}

	md, err := s.BQ.DatasetInProject(s.ProjectID, s.DatasetID).Table(s.TableID).Metadata(ctx)
	if err != nil {
		// Handle authentication errors (e.g., BigQuery emulator doesn't support metadata API)
		// Fall back to compat mode by returning empty strings
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 401 {
			log.Printf("BigQuery metadata call failed with 401 (likely emulator), falling back to compat mode: %v", err)
			return "", "", nil
		}
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
