package postgres

import (
	"context"
	"database/sql"

	"github.com/bxcodec/go-clean-arch/domain"
)

type postgresAuthorRepo struct {
	DB *sql.DB
}

// NewPostgresAuthorRepository will create an implementation of author.Repository for PostgreSQL
func NewPostgresAuthorRepository(db *sql.DB) domain.AuthorRepository {
	return &postgresAuthorRepo{
		DB: db,
	}
}

func (p *postgresAuthorRepo) getOne(ctx context.Context, query string, args ...interface{}) (res domain.Author, err error) {
	stmt, err := p.DB.PrepareContext(ctx, query)
	if err != nil {
		return domain.Author{}, err
	}
	row := stmt.QueryRowContext(ctx, args...)
	res = domain.Author{}

	err = row.Scan(
		&res.ID,
		&res.Name,
		&res.CreatedAt,
		&res.UpdatedAt,
	)
	return res, err
}

func (p *postgresAuthorRepo) GetByID(ctx context.Context, id int64) (domain.Author, error) {
	query := `SELECT id, name, created_at, updated_at FROM author WHERE id=$1`
	return p.getOne(ctx, query, id)
}
