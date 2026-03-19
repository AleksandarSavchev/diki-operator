// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package outputs

import (
	"context"
	"encoding/json"
	"fmt"

	dikireport "github.com/gardener/diki/pkg/report"
	"github.com/jackc/pgx/v5"

	dikiv1alpha1 "github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1"
	"github.com/gardener/diki-operator/pkg/apis/dikiexporter/v1alpha1"
)

type PostgresExporter struct {
	Config dikiv1alpha1.PostgresOutput
}

func NewPostgresExporter(config dikiv1alpha1.PostgresOutput) *PostgresExporter {
	return &PostgresExporter{
		Config: config,
	}
}

func (p *PostgresExporter) Type() v1alpha1.OutputType {
	return v1alpha1.ExporterTypePostgres
}

func (p *PostgresExporter) Export(ctx context.Context, report dikireport.Report) (any, error) {
	conn, err := pgx.Connect(ctx, p.Config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres w/ connectionString `%s`: %w", p.Config.ConnectionString, err)
	}
	defer func(conn *pgx.Conn, ctx context.Context) {
		err := conn.Close(ctx)
		if err != nil {
			panic(fmt.Errorf("failed to close postgres connection: %w", err))
		}
	}(conn, ctx)

	// Prepare the table if it does not exist
	// TODO(tobschli): we need to think about migration strategies for the table in case we want to change the schema in the future. For now, we will just create the table if it does not exist, but this might not be sufficient in the future.
	tableName := pgx.Identifier{p.Config.TableName}.Sanitize()
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			report JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)
	`, tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal report to JSON: %w", err)
	}
	// Insert the report into the table
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (report)
		VALUES ($1)
	`, tableName), reportJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to insert report: %w", err)
	}
	return nil, nil
}
