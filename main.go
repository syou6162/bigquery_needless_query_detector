package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/agnivade/levenshtein"
	"google.golang.org/api/iterator"
)

type BigQueryJob struct {
	CreationTime  time.Time `json:"creation_time"`
	ProjectId     string    `json:"project_id"`
	ProjectNumber int64     `json:"project_number"`
	UserEmail     string    `json:"user_email"`
	JobId         string    `json:"job_id"`
	JobType       string    `json:"job_type"`
	StatementType string    `json:"statement_type"`
	Priority      string    `json:"priority"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	Query         string    `json:"query"`
	State         string    `json:"state"`
	// ReservationId       string
	TotalBytesProcessed int64 `json:"total_bytes_processed"`
	// TotalSlotMs         int64
}

type BigQueryJobsWithStats struct {
	Jobs                []*BigQueryJob `json:"jobs"`
	Count               int            `json:"count"`
	TotalBytesProcessed int64          `json:"total_bytes_processed"`
	Query               string         `json:"query"`
	UserEmail           string         `json:"user_email"`
}

func getMajority(slice []string) string {
	cnt := make(map[string]int)
	for _, s := range slice {
		cnt[s] += 1
	}
	majority := ""
	max := 0
	for k, c := range cnt {
		if c > max {
			majority = k
			max = c
		}
	}
	return majority
}

func getJobClusters(jobs []*BigQueryJob, minThreshold int) []*BigQueryJobsWithStats {
	clusters := make(map[string][]*BigQueryJob)
	if len(jobs) == 0 {
		return nil // , error.Error("")
	}
	clusters[jobs[0].JobId] = []*BigQueryJob{jobs[0]}

	for _, j := range jobs[1:] {
		minDist := 10000
		var minClusterJobId string
		hasExactMatchCluster := false
		for representativeJob, c := range clusters {
			// クエリが同一文字列ならば編集距離を使う必要はない
			if j.Query == c[0].Query {
				clusters[representativeJob] = append(clusters[representativeJob], j)
				hasExactMatchCluster = true
				break
			}
			dist := levenshtein.ComputeDistance(j.Query, c[0].Query)
			if dist < minDist {
				minClusterJobId = representativeJob
				minDist = dist
			}
		}
		if !hasExactMatchCluster {
			// 既存クラスタとの距離が閾値以上のため、新規のクラスタを作る
			if minDist > minThreshold {
				clusters[j.JobId] = []*BigQueryJob{j}
			} else {
				// 既存のクラスタに追加
				clusters[minClusterJobId] = append(clusters[minClusterJobId], j)
			}
		}
	}

	result := make([]*BigQueryJobsWithStats, 0)
	for _, c := range clusters {
		totalBytesProcessed := 0
		users := make([]string, 0)
		for _, j := range c {
			totalBytesProcessed += int(j.TotalBytesProcessed)
			users = append(users, j.UserEmail)
		}
		stats := &BigQueryJobsWithStats{
			Jobs:                c,
			Count:               len(c),
			TotalBytesProcessed: int64(totalBytesProcessed),
			Query:               c[0].Query,
			UserEmail:           getMajority(users),
		}
		result = append(result, stats)
	}

	return result
}

// 以下のジョブの一覧を返すクエリリを生成する関数
// - ジョブが完了しており
// - destination_tableが指定されているが、一定期間以内では使われていない
func generate_query(projectID string, region string, type_ string) string {
	informationSchema := "`" + projectID + "`." + "`region-" + region + "`.INFORMATION_SCHEMA.JOBS_BY_" + type_
	return fmt.Sprintf(`
WITH
  filtered_jobs AS (
  SELECT
    *
  FROM
    %s
  WHERE
    TRUE
    AND job_type = "QUERY"
    AND state = "DONE"
    AND destination_table.project_id IS NOT NULL
    AND NOT STARTS_WITH(destination_table.dataset_id, "_")
  ORDER BY
    total_bytes_processed DESC ),
  referenced_tables AS (
  SELECT
    referenced_tables.project_id,
    referenced_tables.dataset_id,
    referenced_tables.table_id,
  FROM
    %s,
    UNNEST(referenced_tables) AS referenced_tables
  WHERE
    creation_time > "2020-02-01"
  GROUP BY
    referenced_tables.project_id,
    referenced_tables.dataset_id,
    referenced_tables.table_id )
SELECT
  creation_time AS CreationTime,
	project_id AS ProjectId,
	project_number AS ProjectNumber,
	user_email AS UserEmail,
  job_id AS JobId,
	job_type AS JobType,
	statement_type AS StatementType,
	priority AS Priority,
	start_time AS StartTime,
	end_time AS EndTime,
  query AS Query,
	state AS State,
	reservation_id AS ReservationId,
	total_bytes_processed AS TotalBytesProcessed,
	total_slot_ms AS TotalSlotMs,
FROM
  filtered_jobs
WHERE
  destination_table.project_id || ":" || destination_table.dataset_id || "." || destination_table.table_id NOT IN (
		SELECT
			project_id || ":" || dataset_id || "." || table_id
		FROM
			referenced_tables
	)
LIMIT 10000
`,
		informationSchema,
		informationSchema,
	)
}

func getBigQueryJobLogs(projectId string, region string, type_ string) ([]*BigQueryJob, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return nil, err
	}
	query := generate_query(projectId, region, type_)

	iter, err := client.Query(query).Read(ctx)
	if err != nil {
		return nil, err
	}

	jobs := make([]*BigQueryJob, 0)
	for {
		var job BigQueryJob
		err := iter.Next(&job)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func run() {
	var (
		project              = flag.String("project", "", "GCP project")
		region               = flag.String("region", "us", "BigQuery region")
		type_                = flag.String("type", "PROJECT", "PROJECT or ORGANIZATION")
		minDistanceThreshold = flag.Int64("min_distance_threshold", 0, "Minimum query edit distance threshold. This threshold is used to decide two queries are same one.")
	)
	flag.Parse()

	jobs, err := getBigQueryJobLogs(*project, *region, *type_)
	if err != nil {
		fmt.Println(err)
	}

	clusters := getJobClusters(jobs, int(*minDistanceThreshold))

	s, err := json.Marshal(clusters)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(s))
}

func main() {
	run()
}
