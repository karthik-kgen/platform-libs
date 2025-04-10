package repos

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/utils"
	"github.com/redis/go-redis/v9"
)

// getRedisKey returns the Redis key for a specific leaderboard
func (r *ParticipantRepo) getRedisKey(leaderboardID string) string {
	return "leaderboard:" + leaderboardID
}

// setupLeaderboardExpiry sets up the expiry for a leaderboard Redis key
func (r *ParticipantRepo) setupLeaderboardExpiry(
	ctx context.Context,
	redisKey string,
	leaderboardEndTime time.Time,
	pipe redis.Pipeliner,
) {
	// Calculate time until expiry (24 hours after leaderboardEndTime)
	expiryTime := leaderboardEndTime.Add(24 * time.Hour)
	now := utils.GetCurrTimeStamp()

	// Only set expiry if it's in the future
	if expiryTime.After(now) {
		expiryDuration := expiryTime.Sub(now)
		pipe.Expire(ctx, redisKey, expiryDuration)
	}
}

// syncLeaderboard synchronizes the leaderboard data from DynamoDB to Redis
func (r *ParticipantRepo) syncLeaderboard(
	ctx context.Context,
	leaderboardID string,
	pipe redis.Pipeliner,
) error {
	redisKey := r.getRedisKey(leaderboardID)

	// Clear existing sorted set
	pipe.Del(ctx, redisKey)

	// Create a function to process each page of results
	processPage := func(page *dynamodb.QueryOutput, lastPage bool) bool {
		// Unmarshal the items
		var pageItems []map[string]interface{}
		err := attributevalue.UnmarshalListOfMaps(page.Items, &pageItems)
		if err != nil {
			// Log the error but continue processing
			fmt.Printf("Error unmarshaling items: %v\n", err)
			return true
		}

		// Add all items from this page to Redis pipeline
		for _, item := range pageItems {
			namespacedUserID := item["namespacedUserID"].(string)
			score := item["score"].(float64)
			pipe.ZAdd(ctx, redisKey, redis.Z{
				Score:  score,
				Member: namespacedUserID,
			})
		}

		// Continue to the next page
		return !lastPage
	}

	// Create the query input
	input := &dynamodb.QueryInput{
		TableName: aws.String(r.tableName),
		KeyConditionExpression: aws.String(
			"leaderboardID = :lid",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lid": &types.AttributeValueMemberS{
				Value: leaderboardID,
			},
		},
		ProjectionExpression: aws.String(
			"namespacedUserID, score",
		),
	}

	// Use the paginator to handle pagination
	paginator := dynamodb.NewQueryPaginator(r.dynamoClient, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf(
				"failed to query DynamoDB table: %w",
				err,
			)
		}

		// Process the page
		if !processPage(page, !paginator.HasMorePages()) {
			break
		}
	}

	return nil
}

// ensureLeaderboardExists checks if the Redis key exists, creates it if needed, and sets up expiry
func (r *ParticipantRepo) ensureLeaderboardExists(
	ctx context.Context,
	leaderboardID string,
	leaderboardEndTime time.Time,
) error {
	redisKey := r.getRedisKey(leaderboardID)

	// Check if the sorted set exists
	exists, err := r.redisClient.Exists(ctx, redisKey).Result()
	if err != nil {
		return fmt.Errorf(
			"failed to check if Redis key exists: %w",
			err,
		)
	}

	// If the sorted set doesn't exist, try to create it
	if exists == 0 {
		// Create a pipeline for Redis operations
		pipe := r.redisClient.Pipeline()

		// Try to sync data from DynamoDB
		err = r.syncLeaderboard(ctx, leaderboardID, pipe)
		if err != nil {
			// If sync fails, create an empty sorted set
			pipe.ZAdd(ctx, redisKey, redis.Z{})
		}

		// Set up expiry for the leaderboard
		r.setupLeaderboardExpiry(ctx, redisKey, leaderboardEndTime, pipe)

		// Execute all Redis operations
		_, err = pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf(
				"failed to execute Redis pipeline: %w",
				err,
			)
		}
	}

	return nil
}
