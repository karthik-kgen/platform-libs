package repos

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/customTypes"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/models"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/utils"

	"github.com/redis/go-redis/v9"
)

// ParticipantRepo handles data persistence for leaderboard participants
type ParticipantRepo struct {
	dynamoClient *dynamodb.Client
	redisClient  *redis.Client
	tableName    string
}

// NewParticipantRepo creates a new repository instance
func NewParticipantRepo(
	dynamoClient *dynamodb.Client,
	redisClient *redis.Client,
) *ParticipantRepo {
	return &ParticipantRepo{
		dynamoClient: dynamoClient,
		redisClient:  redisClient,
		tableName:    "PlatformLeaderboardScores",
	}
}

// GetTopNParticipants retrieves the top N participants from Redis
func (r *ParticipantRepo) GetTopNParticipants(
	ctx context.Context,
	leaderboardID string,
	n int64,
	leaderboardEndTime time.Time,
) ([]customTypes.MemberScore, error) {
	redisKey := r.getRedisKey(leaderboardID)

	// Ensure the leaderboard exists in Redis
	if err := r.ensureLeaderboardExists(ctx, leaderboardID, leaderboardEndTime); err != nil {
		return nil, err
	}

	// Get top N participants from Redis
	results, err := r.redisClient.ZRevRangeWithScores(
		ctx,
		redisKey,
		0,
		n-1,
	).Result()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get top N participants from Redis: %w",
			err,
		)
	}

	// Convert to MemberScore slice with ranks
	participants := make([]customTypes.MemberScore, len(results))
	for i, result := range results {
		participants[i] = customTypes.MemberScore{
			Member: result.Member.(string),
			Score:  result.Score,
			Rank:   int64(i + 1), // Redis ranks are 0-based, so add 1 for human-readable ranks
		}
	}

	return participants, nil
}

// GetParticipantScoreAndRank retrieves a specific participant's score and rank
// from Redis
func (r *ParticipantRepo) GetParticipantScoreAndRank(
	ctx context.Context,
	leaderboardID string,
	namespacedUserID string,
	leaderboardEndTime time.Time,
) (*customTypes.MemberScore, error) {
	redisKey := r.getRedisKey(leaderboardID)

	// Ensure the leaderboard exists in Redis
	if err := r.ensureLeaderboardExists(ctx, leaderboardID, leaderboardEndTime); err != nil {
		return nil, err
	}

	// Get the participant's score
	score, err := r.redisClient.ZScore(ctx, redisKey, namespacedUserID).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf(
				"participant not found in leaderboard",
			)
		}
		return nil, fmt.Errorf(
			"failed to get participant score: %w",
			err,
		)
	}

	// Get the participant's rank (0-based, so add 1 for human-readable rank)
	rank, err := r.redisClient.ZRevRank(ctx, redisKey, namespacedUserID).Result()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get participant rank: %w",
			err,
		)
	}

	return &customTypes.MemberScore{
		Member: namespacedUserID,
		Score:  score,
		Rank:   rank + 1, // Convert to 1-based rank
	}, nil
}

// UpdateScore updates a participant's score in both DynamoDB and Redis
func (r *ParticipantRepo) UpdateScore(
	ctx context.Context,
	leaderboardID string,
	namespacedUserID string,
	scoreDelta float64,
	leaderboardEndTime time.Time,
) error {
	redisKey := r.getRedisKey(leaderboardID)

	dynamoKey, err := attributevalue.MarshalMap(map[string]interface{}{
		"leaderboardID":    leaderboardID,
		"namespacedUserID": namespacedUserID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal key: %w", err)
	}

	now := utils.GetCurrTimeStamp()

	// Prepare update expression and attribute values
	updateExpression := "SET score = if_not_exists(score, :zero) + :incVal, updated_at = :updatedAt"
	expressionAttributeValues := make(map[string]types.AttributeValue)
	expressionAttributeValues[":incVal"] = &types.AttributeValueMemberN{
		Value: fmt.Sprintf("%f", scoreDelta),
	}
	expressionAttributeValues[":zero"] = &types.AttributeValueMemberN{
		Value: "0",
	}
	expressionAttributeValues[":updatedAt"] = &types.AttributeValueMemberN{
		Value: now.Format(time.RFC3339),
	}

	// Update DynamoDB
	_, err = r.dynamoClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.tableName),
		Key:                       dynamoKey,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to update score in DynamoDB: %w",
			err,
		)
	}

	// Create a pipeline for Redis operations
	pipe := r.redisClient.Pipeline()

	// Update Redis sorted set
	pipe.ZIncrBy(ctx, redisKey, scoreDelta, namespacedUserID)

	// Ensure Redis key exists and has proper expiry
	if err := r.ensureLeaderboardExists(ctx, leaderboardID, leaderboardEndTime); err != nil {
		return err
	}

	// Execute all Redis operations
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to update Redis sorted set: %w",
			err,
		)
	}

	return nil
}

// JoinLeaderboard adds a participant to the leaderboard
func (r *ParticipantRepo) JoinLeaderboard(
	ctx context.Context,
	participant *models.ParticipantModel,
	leaderboardEndTime time.Time,
) error {
	redisKey := r.getRedisKey(participant.LeaderboardID)

	// Check if the participant already exists in DynamoDB
	dynamoKey, err := attributevalue.MarshalMap(map[string]interface{}{
		"leaderboardID":    participant.LeaderboardID,
		"namespacedUserID": participant.NamespacedUserID,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to marshal key: %w",
			err,
		)
	}

	// Check if the participant exists
	_, err = r.dynamoClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key:       dynamoKey,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to check if participant exists: %w",
			err,
		)
	}

	// Update the participant's timestamp
	participant.UpdatedAt = utils.GetCurrTimeStamp()

	// Marshal the participant model directly
	item, err := attributevalue.MarshalMap(participant)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal participant model: %w",
			err,
		)
	}

	// Add created_at field
	item["created_at"] = &types.AttributeValueMemberN{
		Value: fmt.Sprintf("%d", participant.UpdatedAt.Unix()),
	}

	// Put the item in DynamoDB
	_, err = r.dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to put item in DynamoDB: %w",
			err,
		)
	}

	// Create a pipeline for Redis operations
	pipe := r.redisClient.Pipeline()

	// Add the participant to the Redis sorted set
	pipe.ZAdd(ctx, redisKey, redis.Z{
		Score:  participant.Score,
		Member: participant.NamespacedUserID,
	})

	// Ensure Redis key exists and has proper expiry
	if err := r.ensureLeaderboardExists(ctx, participant.LeaderboardID, leaderboardEndTime); err != nil {
		return err
	}

	// Execute all Redis operations
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to update Redis sorted set: %w",
			err,
		)
	}

	return nil
}

// LeaveLeaderboard removes a participant from the leaderboard
func (r *ParticipantRepo) LeaveLeaderboard(
	ctx context.Context,
	leaderboardID string,
	namespacedUserID string,
) error {
	redisKey := r.getRedisKey(leaderboardID)

	// Create a pipeline for Redis operations
	pipe := r.redisClient.Pipeline()

	// Remove the participant from the Redis sorted set
	pipe.ZRem(ctx, redisKey, namespacedUserID)

	// Execute Redis operations
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to remove participant from Redis sorted set: %w",
			err,
		)
	}

	// Remove the participant from DynamoDB
	dynamoKey, err := attributevalue.MarshalMap(map[string]interface{}{
		"leaderboardID":    leaderboardID,
		"namespacedUserID": namespacedUserID,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to marshal key: %w",
			err,
		)
	}

	_, err = r.dynamoClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(r.tableName),
		Key:       dynamoKey,
	})
	if err != nil {
		return fmt.Errorf(
			"failed to delete participant from DynamoDB: %w",
			err,
		)
	}

	return nil
}
