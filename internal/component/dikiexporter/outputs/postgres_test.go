// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package outputs_test

import (
	"context"
	"fmt"
	"net"
	"time"

	dikireport "github.com/gardener/diki/pkg/report"
	"github.com/jackc/pgx/v5/pgproto3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/gardener/diki-operator/internal/component/dikiexporter/outputs"
	dikiv1alpha1 "github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1"
)

// mockPostgresServer starts a TCP listener that speaks enough of the Postgres wire
// protocol to let pgx connect, execute two Exec statements, and close cleanly.
// The script function receives the backend and is responsible for handling the
// authentication handshake and both query round-trips.
func mockPostgresServer(script func(backend *pgproto3.Backend) error) (connStr string, cleanup func(), serverErr <-chan error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).ToNot(HaveOccurred())

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close() //nolint:errcheck
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		backend := pgproto3.NewBackend(conn, conn)
		if err := script(backend); err != nil {
			errCh <- err
		}
	}()

	host, port, _ := net.SplitHostPort(ln.Addr().String())
	connStr = fmt.Sprintf("host=%s port=%s sslmode=disable user=test dbname=test", host, port)
	cleanup = func() { ln.Close() } //nolint:errcheck
	serverErr = errCh
	return
}

// acceptConn handles the startup / authentication handshake.
func acceptConn(backend *pgproto3.Backend) error {
	_, err := backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("receive startup: %w", err)
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return backend.Flush()
}

// handleSimpleExec receives a simple Query message (no parameters) and responds with success.
// Used for queries built with fmt.Sprintf that contain no $N placeholders.
func handleSimpleExec(backend *pgproto3.Backend) error {
	msg, err := backend.Receive()
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}
	if _, ok := msg.(*pgproto3.Query); !ok {
		return fmt.Errorf("expected Query, got %T", msg)
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("CREATE TABLE")})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return backend.Flush()
}

// handleExec receives one extended-query round-trip for a parameterized query.
// In QueryExecModeCacheStatement (pgx default), pgx first sends Parse+Describe+Sync
// to prepare the statement, then Bind+Execute+Sync to run it.
func handleExec(backend *pgproto3.Backend) error {
	// Phase 1: Prepare (Parse + Describe + Sync)
	for {
		msg, err := backend.Receive()
		if err != nil {
			return fmt.Errorf("receive prepare: %w", err)
		}
		switch msg.(type) {
		case *pgproto3.Parse:
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			// One parameter ($1), unknown OID (0 = untyped)
			backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: []uint32{0}})
			backend.Send(&pgproto3.NoData{})
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				return fmt.Errorf("flush prepare: %w", err)
			}
			goto execute
		}
	}
execute:
	// Phase 2: Execute (Bind + Execute + Sync)
	for {
		msg, err := backend.Receive()
		if err != nil {
			return fmt.Errorf("receive execute: %w", err)
		}
		switch msg.(type) {
		case *pgproto3.Bind:
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Execute:
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return backend.Flush()
		}
	}
}

// handleClose waits for the client to send a Terminate message.
func handleClose(backend *pgproto3.Backend) error {
	for {
		msg, err := backend.Receive()
		if err != nil {
			// EOF or connection reset means the client closed – that is fine.
			return nil
		}
		if _, ok := msg.(*pgproto3.Terminate); ok {
			return nil
		}
	}
}

var _ = Describe("PostgresExporter", func() {
	var (
		ctx = logf.IntoContext(context.Background(), logzap.New(logzap.WriteTo(GinkgoWriter)))

		dikiReport *dikireport.Report
		pgExporter outputs.PostgresExporter
	)

	BeforeEach(func() {
		dikiReport = &dikireport.Report{
			Providers: []dikireport.Provider{
				{
					ID:   "FAKE",
					Name: "FAKE",
					Rulesets: []dikireport.Ruleset{
						{
							ID:   "FAKE",
							Name: "FAKE",
						},
					},
				},
			},
		}
	})

	It("should insert the Diki report into Postgres", func() {
		connStr, cleanup, serverErr := mockPostgresServer(func(backend *pgproto3.Backend) error {
			if err := acceptConn(backend); err != nil {
				return err
			}
			// First Exec: CREATE TABLE IF NOT EXISTS (simple query, no params)
			if err := handleSimpleExec(backend); err != nil {
				return err
			}
			// Second Exec: INSERT INTO (extended query with $1 param)
			if err := handleExec(backend); err != nil {
				return err
			}
			return handleClose(backend)
		})
		defer cleanup()

		pgExporter = outputs.PostgresExporter{
			Config: dikiv1alpha1.PostgresOutput{
				ConnectionString: connStr,
				TableName:        "diki_reports",
			},
		}

		details, err := pgExporter.Export(ctx, *dikiReport)
		Expect(err).ToNot(HaveOccurred())
		Expect(details).To(BeNil())

		Expect(<-serverErr).To(BeNil())
	})

	It("should use the configured table name", func() {
		connStr, cleanup, serverErr := mockPostgresServer(func(backend *pgproto3.Backend) error {
			if err := acceptConn(backend); err != nil {
				return err
			}
			if err := handleSimpleExec(backend); err != nil {
				return err
			}
			if err := handleExec(backend); err != nil {
				return err
			}
			return handleClose(backend)
		})
		defer cleanup()

		pgExporter = outputs.PostgresExporter{
			Config: dikiv1alpha1.PostgresOutput{
				ConnectionString: connStr,
				TableName:        "custom_reports",
			},
		}

		details, err := pgExporter.Export(ctx, *dikiReport)
		Expect(err).ToNot(HaveOccurred())
		Expect(details).To(BeNil())

		Expect(<-serverErr).To(BeNil())
	})

	It("should return an error when the connection fails", func() {
		pgExporter = outputs.PostgresExporter{
			Config: dikiv1alpha1.PostgresOutput{
				ConnectionString: "host=127.0.0.1 port=1 sslmode=disable user=test dbname=test connect_timeout=1",
				TableName:        "diki_reports",
			},
		}

		_, err := pgExporter.Export(ctx, *dikiReport)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to connect to postgres"))
	})
})
