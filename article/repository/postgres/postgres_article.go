package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/bxcodec/go-clean-arch/article/repository"
	"github.com/bxcodec/go-clean-arch/domain"
)

type postgresArticleRepository struct {
	Conn *sql.DB
}

// NewPostgresArticleRepository will create an object that represents the article.Repository interface
func NewPostgresArticleRepository(conn *sql.DB) domain.ArticleRepository {
	return &postgresArticleRepository{conn}
}

func (p *postgresArticleRepository) fetch(ctx context.Context, query string, args ...interface{}) (result []domain.Article, err error) {
	rows, err := p.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}

	defer func() {
		errRow := rows.Close()
		if errRow != nil {
			logrus.Error(errRow)
		}
	}()

	result = make([]domain.Article, 0)
	for rows.Next() {
		t := domain.Article{}
		authorID := int64(0)
		err = rows.Scan(
			&t.ID,
			&t.Title,
			&t.Content,
			&authorID,
			&t.UpdatedAt,
			&t.CreatedAt,
		)

		if err != nil {
			logrus.Error(err)
			return nil, err
		}
		t.Author = domain.Author{
			ID: authorID,
		}
		result = append(result, t)
	}

	return result, nil
}

func (p *postgresArticleRepository) Fetch(ctx context.Context, cursor string, num int64) (res []domain.Article, nextCursor string, err error) {
	query := `SELECT id,title,content, author_id, updated_at, created_at
  					  FROM article WHERE created_at > $1 ORDER BY created_at LIMIT $2`

	decodedCursor, err := repository.DecodeCursor(cursor)
	if err != nil && cursor != "" {
		return nil, "", domain.ErrBadParamInput
	}

	res, err = p.fetch(ctx, query, decodedCursor, num)
	if err != nil {
		return nil, "", err
	}

	if len(res) == int(num) {
		nextCursor = repository.EncodeCursor(res[len(res)-1].CreatedAt)
	}

	return
}

func (p *postgresArticleRepository) GetByID(ctx context.Context, id int64) (res domain.Article, err error) {
	query := `SELECT id,title,content, author_id, updated_at, created_at
  					  FROM article WHERE ID = $1`

	list, err := p.fetch(ctx, query, id)
	if err != nil {
		return domain.Article{}, err
	}

	if len(list) > 0 {
		res = list[0]
	} else {
		return res, domain.ErrNotFound
	}

	return
}

func (p *postgresArticleRepository) GetByTitle(ctx context.Context, title string) (res domain.Article, err error) {
	query := `SELECT id,title,content, author_id, updated_at, created_at
  					  FROM article WHERE title = $1`

	list, err := p.fetch(ctx, query, title)
	if err != nil {
		return
	}

	if len(list) > 0 {
		res = list[0]
	} else {
		return res, domain.ErrNotFound
	}
	return
}

func (p *postgresArticleRepository) Store(ctx context.Context, a *domain.Article) (err error) {
	query := `INSERT INTO article (title, content, author_id, updated_at, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id`

	stmt, err := p.Conn.PrepareContext(ctx, query)
	if err != nil {
		return
	}

	err = stmt.QueryRowContext(ctx, a.Title, a.Content, a.Author.ID, a.UpdatedAt, a.CreatedAt).Scan(&a.ID)
	if err != nil {
		return
	}

	return
}

func (p *postgresArticleRepository) Delete(ctx context.Context, id int64) (err error) {
	query := "DELETE FROM article WHERE id = $1"

	stmt, err := p.Conn.PrepareContext(ctx, query)
	if err != nil {
		return
	}

	res, err := stmt.ExecContext(ctx, id)
	if err != nil {
		return
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return
	}

	if rowsAffected != 1 {
		err = fmt.Errorf("unexpected behavior. Total Affected: %d", rowsAffected)
		return
	}

	return
}

func (p *postgresArticleRepository) Update(ctx context.Context, ar *domain.Article) (err error) {
	query := `UPDATE article SET title=$1, content=$2, author_id=$3, updated_at=$4 WHERE ID = $5`

	stmt, err := p.Conn.PrepareContext(ctx, query)
	if err != nil {
		return
	}

	res, err := stmt.ExecContext(ctx, ar.Title, ar.Content, ar.Author.ID, ar.UpdatedAt, ar.ID)
	if err != nil {
		return
	}

	affect, err := res.RowsAffected()
	if err != nil {
		return
	}

	if affect != 1 {
		err = fmt.Errorf("unexpected behavior. Total Affected: %d", affect)
		return
	}

	return
}
