package leaderboard

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/customTypes"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/models"
	"github.com/kgen-protocol/platform-libs/leaderboard/internal/repos"
	"github.com/redis/go-redis/v9"
)

// IndividualLeaderboardHelper handles the business logic for leaderboard operations
type IndividualLeaderboardHelper struct {
	repo               *repos.ParticipantRepo
	clientID           string
	leaderboardID      string
	leaderboardEndTime time.Time
}

// NewIndividualLeaderboardHelper creates a new leaderboard service instance
func NewIndividualLeaderboardHelper(
	dynamoClient *dynamodb.Client,
	redisClient *redis.Client,
	clientID string,
	leaderboardID string,
	leaderboardEndTime time.Time,
) *IndividualLeaderboardHelper {
	repo := repos.NewParticipantRepo(dynamoClient, redisClient)
	return &IndividualLeaderboardHelper{
		repo:               repo,
		clientID:           clientID,
		leaderboardID:      leaderboardID,
		leaderboardEndTime: leaderboardEndTime,
	}
}

// validateNamespacedUserID validates and splits the namespacedUserID
func (l *IndividualLeaderboardHelper) validateNamespacedUserID(
	namespacedUserID string,
) (string, string, error) {
	clientID, userID := models.SplitNamespacedUserID(namespacedUserID)
	if clientID == "" || userID == "" {
		return "", "", fmt.Errorf("invalid namespaced user ID format")
	}

	return clientID, userID, nil
}

// UpdateScore updates a participant's score in the leaderboard
func (l *IndividualLeaderboardHelper) UpdateScore(
	ctx context.Context,
	namespacedUserID string,
	scoreDelta float64,
) error {
	_, userID, err := l.validateNamespacedUserID(namespacedUserID)
	if err != nil {
		return err
	}

	participant := models.NewParticipantModel(
		l.leaderboardID,
		l.clientID,
		userID,
		scoreDelta,
	)
	return l.repo.UpdateScore(
		ctx,
		l.leaderboardID,
		participant.NamespacedUserID,
		participant.Score,
		l.leaderboardEndTime,
	)
}

// GetTopNParticipants retrieves the top N participants from the leaderboard
func (l *IndividualLeaderboardHelper) GetTopNParticipants(ctx context.Context, n int64) ([]customTypes.MemberScore, error) {
	return l.repo.GetTopNParticipants(
		ctx,
		l.leaderboardID,
		n,
		l.leaderboardEndTime,
	)
}

// GetParticipantScoreAndRank retrieves a specific participant's score and rank
// from the leaderboard
func (l *IndividualLeaderboardHelper) GetParticipantScoreAndRank(
	ctx context.Context,
	namespacedUserID string,
) (*customTypes.MemberScore, error) {
	_, _, err := l.validateNamespacedUserID(namespacedUserID)
	if err != nil {
		return nil, err
	}

	return l.repo.GetParticipantScoreAndRank(
		ctx,
		l.leaderboardID,
		namespacedUserID,
		l.leaderboardEndTime,
	)
}
