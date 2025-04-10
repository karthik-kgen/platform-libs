package models

import (
	"strings"
	"time"

	"github.com/kgen-protocol/platform-libs/leaderboard/internal/utils"
)

// Participant represents a user's score in a leaderboard.
type ParticipantModel struct {
	LeaderboardID    string    `json:"leaderboardID" dynamodbav:"leaderboardID"`
	NamespacedUserID string    `json:"namespacedUserID" dynamodbav:"namespacedUserID"`
	ClientID         string    `json:"clientID" dynamodbav:"clientID"`
	UserID           string    `json:"userID" dynamodbav:"userID"`
	Score            float64   `json:"score" dynamodbav:"score"`
	UpdatedAt        time.Time `json:"updatedAt" dynamodbav:"updatedAt"`
}

// NewParticipant creates a new participant with the given parameters
func NewParticipantModel(leaderboardID, clientID, userID string, score float64) *ParticipantModel {
	namespacedUserID := CreateNamespacedUserID(clientID, userID)
	return &ParticipantModel{
		LeaderboardID:    leaderboardID,
		NamespacedUserID: namespacedUserID,
		ClientID:         clientID,
		UserID:           userID,
		Score:            score,
		UpdatedAt:        utils.GetCurrTimeStamp(),
	}
}

// NewParticipantFromNamespacedID creates a new participant from a namespaced user ID
func NewParticipantFromNamespacedID(leaderboardID, namespacedUserID string, score float64) *ParticipantModel {
	clientID, userID := SplitNamespacedUserID(namespacedUserID)
	return &ParticipantModel{
		LeaderboardID:    leaderboardID,
		NamespacedUserID: namespacedUserID,
		ClientID:         clientID,
		UserID:           userID,
		Score:            score,
		UpdatedAt:        utils.GetCurrTimeStamp(),
	}
}

// CreateNamespacedUserID combines clientID and userID into the expected format
func CreateNamespacedUserID(clientID, userID string) string {
	return clientID + "___" + userID
}

// SplitNamespacedUserID splits a combined user ID into clientID and userID
// The format is expected to be "clientID___userID"
func SplitNamespacedUserID(namespacedUserID string) (clientID, userID string) {
	parts := strings.Split(namespacedUserID, "___")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}

	return "", ""
}
