#pragma once
#include <string>
#include <vector>
#include <cstdint>

// =============================================================================
// Topic names — all pre-created by the admin on Confluent Cloud.
// Players never create topics; they only produce and consume.
// =============================================================================
namespace KTopic {
    constexpr const char* SESSIONS = "kafkatanx-sessions";  // compacted — session registry
    constexpr const char* GAMEPLAY = "kafkatanx-gameplay";  // operational — replaces TCP pipe
    constexpr const char* PLAYERS  = "kafkatanx-players";   // compacted — player identity
    constexpr const char* SHOTS    = "kafkatanx-shots";     // analytics — one event per shot
    constexpr const char* ROUNDS   = "kafkatanx-rounds";    // analytics — one event per round
    constexpr const char* GAMES    = "kafkatanx-games";     // analytics — one event per match
}

// =============================================================================
// KafkaConfig — loaded from client-kafka.ini on startup.
// The ini is committed to git with real restricted credentials pre-filled.
// player.id is auto-generated on first run; player.name is prompted in-game.
// =============================================================================
struct KafkaConfig {
    // [kafka]
    std::string bootstrapServers;
    std::string saslUsername;
    std::string saslPassword;

    // [schema-registry]
    std::string schemaRegistryUrl;
    std::string schemaRegistryUserInfo;  // "api-key:api-secret"

    // [schema-ids] — assigned by Confluent Schema Registry after admin registers .avsc files.
    // Fill these in after running: confluent schema-registry schema create ...
    int32_t schemaIdSession  = 0;
    int32_t schemaIdGameplay = 0;
    int32_t schemaIdPlayer   = 0;
    int32_t schemaIdShot     = 0;
    int32_t schemaIdRound    = 0;
    int32_t schemaIdGame     = 0;

    // [player]
    std::string playerId;    // UUID v4, auto-generated on first launch
    std::string playerName;  // display name, prompted in-game if blank

    static KafkaConfig LoadFromFile(const std::string& path);
    void SaveToFile(const std::string& path) const;

    bool IsValid() const;           // credentials present and non-empty
    bool HasPlayerIdentity() const; // player.id and player.name both set

    void GeneratePlayerId();                        // UUID v4 using random_device
    static std::string GenerateGameCode();          // 6-char unambiguous alphanumeric
};

// =============================================================================
// KafkaMsg — a message received from kafkatanx-gameplay.
// The payload field carries the raw game bytes in the same NetBuf format
// used by the original tanx TCP code — no changes to game logic needed.
// =============================================================================
struct KafkaMsg {
    std::string gameCode;
    std::string senderPlayerId;
    int64_t     sequence    = 0;
    int32_t     messageType = 0;           // maps to the NetMsg enum in game code
    std::vector<uint8_t> payload;          // raw game bytes
};

// =============================================================================
// Analytics event structs — filled by game logic in kafkatanx.cpp.
// Only the HOST produces these; they represent canonical authoritative state.
// =============================================================================
struct ShotEventData {
    std::string gameCode;
    int round = 0, turn = 0;
    std::string shooterPlayerId, shooterName;
    std::string targetPlayerId,  targetName;
    std::string weapon;                       // "NORMAL","HE","CLUSTER","LASER"
    float  angle = 0, power = 0, windSpeed = 0;
    std::string gravitySetting, landscapeSetting;
    bool   nightMode = false, hit = false;
    int    damageDealt = 0, targetHpBefore = 0, targetHpAfter = 0;
    float  craterX = 0, craterY = 0;         // (0,0) if miss
    int64_t shotAt = 0;                       // epoch millis
};

struct RoundEventData {
    std::string gameCode;
    int   roundNumber = 0, turnsTaken = 0, shotsFired = 0;
    std::string winnerPlayerId, winnerName;   // empty string = draw
    std::string drawReason;                   // "STALEMATE" | "MUTUAL_KILL" | ""
    float durationSeconds = 0;
    int64_t endedAt = 0;
};

struct GameEventData {
    std::string gameCode;
    std::string hostPlayerId, hostName;
    std::string clientPlayerId, clientName;
    std::string winnerPlayerId, winnerName;   // empty string = draw
    int   roundsPlayed = 0, totalTurns = 0;
    std::string windSetting, gravitySetting, landscapeSetting;
    bool  nightMode = false;
    int   roundsToWin = 0, startingHp = 0;
    int64_t startedAt = 0, endedAt = 0;
    float durationSeconds = 0;
};

// =============================================================================
// KafkaNet — the Kafka network layer, replacing the TCP socket code.
//
// HOST flow:
//   Init() → PublishPlayerProfile() → PublishSessionWaiting(code)
//   → SubscribeGameplay(code, playerId)
//   → SendGameplay(MATCH_START) → SendGameplay(ROUND_START)
//   → per turn: receive TURN_ACTION → SendGameplay(TURN_RESULT)
//   → PublishShot() / PublishRound() / PublishGame() [analytics]
//   → PublishSessionComplete() → Shutdown()
//
// CLIENT flow:
//   Init() → PublishPlayerProfile() → SubscribeGameplay(code, playerId)
//   → receive MATCH_START / ROUND_START
//   → per turn: SendGameplay(TURN_ACTION) → receive TURN_RESULT
//   → Shutdown()
// =============================================================================
class KafkaNet {
public:
    KafkaNet()  = default;
    ~KafkaNet();

    // Lifecycle
    bool Init(const KafkaConfig& cfg);
    void Shutdown();
    bool IsReady()    const { return ready_; }
    std::string LastError() const { return lastError_; }

    // Session management (kafkatanx-sessions)
    bool PublishSessionWaiting(const std::string& gameCode);
    bool PublishSessionActive(const std::string& gameCode);
    bool PublishSessionComplete(const std::string& gameCode);

    // Gameplay pipe (kafkatanx-gameplay) — direct replacement for TCP send/recv
    bool SendGameplay(const std::string& gameCode, int32_t msgType,
                      const std::vector<uint8_t>& payload);
    bool PollGameplay(KafkaMsg& out, int timeoutMs = 100);
    void SubscribeGameplay(const std::string& gameCode, const std::string& playerId);
    void UnsubscribeGameplay(); // closes the consumer without touching the producer

    // Player identity (kafkatanx-players)
    bool PublishPlayerProfile();

    // Analytics — HOST only, called after authoritative resolution
    bool PublishShot(const ShotEventData& ev);
    bool PublishRound(const RoundEventData& ev);
    bool PublishGame(const GameEventData& ev);

private:
    KafkaConfig cfg_;
    bool        ready_    = false;
    std::string lastError_;
    int64_t     sendSeq_  = 0;

    // Opaque rdkafka handles — cast to RdKafka::Producer* / RdKafka::KafkaConsumer*
    // in kafka_net.cpp. Kept void* here to avoid pulling rdkafka headers into game code.
    void* producer_ = nullptr;
    void* consumer_ = nullptr;
};
