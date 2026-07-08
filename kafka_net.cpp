#include "kafka_net.h"
#include "avro_codec.h"
#include "logger.h"
#include <librdkafka/rdkafkacpp.h>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <random>
#include <chrono>
#include <ctime>
#include <cstdio>

// =============================================================================
// INI parser helpers
// =============================================================================

static std::string IniTrim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    size_t b = s.find_last_not_of(" \t\r\n");
    return (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
}

KafkaConfig KafkaConfig::LoadFromFile(const std::string& path) {
    KafkaConfig cfg;
    std::ifstream f(path);
    if (!f.is_open()) return cfg;

    std::string section, line;
    while (std::getline(f, line)) {
        line = IniTrim(line);
        if (line.empty() || line[0] == ';' || line[0] == '#') continue;
        if (line[0] == '[') {
            size_t e = line.find(']');
            if (e != std::string::npos) section = line.substr(1, e - 1);
            continue;
        }
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = IniTrim(line.substr(0, eq));
        std::string val = IniTrim(line.substr(eq + 1));

        if (section == "kafka") {
            if      (key == "bootstrap.servers") cfg.bootstrapServers = val;
            else if (key == "sasl.username")     cfg.saslUsername     = val;
            else if (key == "sasl.password")     cfg.saslPassword     = val;
        } else if (section == "schema-registry") {
            if      (key == "url")                   cfg.schemaRegistryUrl      = val;
            else if (key == "basic.auth.user.info")  cfg.schemaRegistryUserInfo = val;
        } else if (section == "schema-ids") {
            try {
                if      (key == "session")  cfg.schemaIdSession  = std::stoi(val);
                else if (key == "gameplay") cfg.schemaIdGameplay = std::stoi(val);
                else if (key == "player")   cfg.schemaIdPlayer   = std::stoi(val);
                else if (key == "shot")     cfg.schemaIdShot     = std::stoi(val);
                else if (key == "round")    cfg.schemaIdRound    = std::stoi(val);
                else if (key == "game")     cfg.schemaIdGame     = std::stoi(val);
            } catch (...) {}
        } else if (section == "player") {
            if      (key == "player.id")   cfg.playerId    = val;
            else if (key == "player.name") cfg.playerName  = val;
        }
    }
    return cfg;
}

void KafkaConfig::SaveToFile(const std::string& path) const {
    std::ofstream f(path);
    if (!f.is_open()) return;
    f << "[kafka]\n"
      << "bootstrap.servers=" << bootstrapServers << "\n"
      << "security.protocol=SASL_SSL\n"
      << "sasl.mechanisms=PLAIN\n"
      << "sasl.username=" << saslUsername << "\n"
      << "sasl.password=" << saslPassword << "\n\n"

      << "[schema-registry]\n"
      << "url=" << schemaRegistryUrl << "\n"
      << "basic.auth.credentials.source=USER_INFO\n"
      << "basic.auth.user.info=" << schemaRegistryUserInfo << "\n\n"

      << "[schema-ids]\n"
      << "; Populated by admin after registering schemas/  *.avsc files\n"
      << "session="  << schemaIdSession  << "\n"
      << "gameplay=" << schemaIdGameplay << "\n"
      << "player="   << schemaIdPlayer   << "\n"
      << "shot="     << schemaIdShot     << "\n"
      << "round="    << schemaIdRound    << "\n"
      << "game="     << schemaIdGame     << "\n\n"

      << "[player]\n"
      << "; Auto-generated on first launch — do not edit player.id\n"
      << "player.id="   << playerId    << "\n"
      << "player.name=" << playerName  << "\n";
}

bool KafkaConfig::IsValid() const {
    return !bootstrapServers.empty() && !saslUsername.empty() && !saslPassword.empty();
}

bool KafkaConfig::HasPlayerIdentity() const {
    return !playerId.empty() && !playerName.empty();
}

void KafkaConfig::GeneratePlayerId() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    uint32_t a  = dis(gen);
    uint32_t b  = dis(gen) & 0xFFFF;
    uint32_t c  = (dis(gen) & 0x0FFF) | 0x4000;  // version 4
    uint32_t d  = (dis(gen) & 0x3FFF) | 0x8000;  // variant bits
    uint32_t e1 = dis(gen);
    uint32_t e2 = dis(gen) & 0xFFFF;
    char buf[37];
    snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%08x%04x",
             a, b, c, d, e1, e2);
    playerId = buf;
}

std::string KafkaConfig::GenerateGameCode() {
    // Unambiguous characters — no I, O, 0, 1 to avoid reading errors
    static const char chars[] = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, 31);
    std::string code;
    for (int i = 0; i < 6; i++) code += chars[dis(gen)];
    return code;
}

// =============================================================================
// KafkaNet implementation
// =============================================================================

KafkaNet::~KafkaNet() { Shutdown(); }

// ---------------------------------------------------------------------------
// Error-surfacing callbacks. Without these, connection/auth failures and
// produce-time delivery failures are invisible to the app — only visible via
// librdkafka's own stderr logging, which nobody running a packaged binary
// with no attached terminal will ever see.
// ---------------------------------------------------------------------------

namespace {

class KafkaEventCb : public RdKafka::EventCb {
public:
    explicit KafkaEventCb(std::string* errorSink) : errorSink_(errorSink) {}
    void event_cb(RdKafka::Event& event) override {
        if (event.type() == RdKafka::Event::EVENT_ERROR) {
            *errorSink_ = "Kafka connection error: " + RdKafka::err2str(event.err());
            NetLog::Write("EVENT_ERROR " + RdKafka::err2str(event.err()) + ": " + event.str());
        }
    }
private:
    std::string* errorSink_;
};

class KafkaDeliveryCb : public RdKafka::DeliveryReportCb {
public:
    explicit KafkaDeliveryCb(std::string* errorSink) : errorSink_(errorSink) {}
    void dr_cb(RdKafka::Message& message) override {
        if (message.err() != RdKafka::ERR_NO_ERROR) {
            *errorSink_ = "Message delivery failed: " + message.errstr();
            NetLog::Write("DELIVERY FAILED: " + message.errstr());
        }
    }
private:
    std::string* errorSink_;
};

// Without a rebalance_cb, librdkafka silently assigns/revokes partitions on
// our behalf — if the broker ever evicts this consumer from its (unique,
// single-member) group, message delivery would just stop with nothing in
// the log to explain why. This registers our own handler purely for
// visibility and mirrors librdkafka's default assign/unassign behavior
// exactly, so consumption itself is unaffected.
class KafkaRebalanceCb : public RdKafka::RebalanceCb {
public:
    void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                       std::vector<RdKafka::TopicPartition*>& partitions) override {
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            NetLog::Write("Consumer group rebalance: partitions ASSIGNED (" +
                          std::to_string(partitions.size()) + ")");
            consumer->assign(partitions);
        } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
            NetLog::Write("Consumer group rebalance: partitions REVOKED (" +
                          std::to_string(partitions.size()) + ")");
            consumer->unassign();
        } else {
            NetLog::Write("Consumer group rebalance error: " + RdKafka::err2str(err));
        }
    }
};

}  // namespace

// Builds a base rdkafka configuration with SASL_SSL credentials.
static RdKafka::Conf* MakeBaseConf(const KafkaConfig& cfg, std::string& err,
                                    RdKafka::EventCb* eventCb) {
    auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    auto set = [&](const char* k, const std::string& v) -> bool {
        return conf->set(k, v, err) == RdKafka::Conf::CONF_OK;
    };
    if (!set("bootstrap.servers", cfg.bootstrapServers) ||
        !set("security.protocol", "SASL_SSL")           ||
        !set("sasl.mechanisms",   "PLAIN")              ||
        !set("sasl.username",     cfg.saslUsername)     ||
        !set("sasl.password",     cfg.saslPassword)     ||
        // Home routers/NAT/firewalls commonly kill idle outbound TCP after
        // 30-60s, which the lobby screen's mostly-idle wait easily exceeds.
        // Without keepalives the dead connection isn't noticed until a real
        // request times out (~60s later), which reads as a silent "no ack".
        !set("socket.keepalive.enable", "true")) {
        delete conf;
        return nullptr;
    }
    conf->set("event_cb", eventCb, err);
    return conf;
}

bool KafkaNet::Init(const KafkaConfig& cfg) {
    cfg_ = cfg;
    std::string err;

    auto* eventCb     = new KafkaEventCb(&lastError_);
    auto* deliveryCb  = new KafkaDeliveryCb(&lastError_);
    auto* rebalanceCb = new KafkaRebalanceCb();
    eventCb_     = eventCb;
    deliveryCb_  = deliveryCb;
    rebalanceCb_ = rebalanceCb;

    auto* pconf = MakeBaseConf(cfg, err, eventCb);
    if (!pconf) { lastError_ = "Producer conf: " + err; NetLog::Write("Init FAILED: producer conf: " + err); return false; }
    pconf->set("dr_cb", deliveryCb, err);

    auto* prod = RdKafka::Producer::create(pconf, err);
    delete pconf;
    if (!prod) { lastError_ = "Producer create: " + err; NetLog::Write("Init FAILED: producer create: " + err); return false; }

    producer_ = prod;
    ready_    = true;
    NetLog::Write("Init OK — producer ready, bootstrap=" + cfg.bootstrapServers);
    return true;
}

void KafkaNet::Shutdown() {
    if (consumer_) {
        auto* c = static_cast<RdKafka::KafkaConsumer*>(consumer_);
        c->close();
        delete c;
        consumer_ = nullptr;
    }
    if (producer_) {
        auto* p = static_cast<RdKafka::Producer*>(producer_);
        p->flush(5000);
        delete p;
        producer_ = nullptr;
    }
    delete static_cast<RdKafka::EventCb*>(eventCb_);
    eventCb_ = nullptr;
    delete static_cast<RdKafka::DeliveryReportCb*>(deliveryCb_);
    deliveryCb_ = nullptr;
    delete static_cast<RdKafka::RebalanceCb*>(rebalanceCb_);
    rebalanceCb_ = nullptr;
    ready_ = false;
    NetLog::Write("Shutdown");
}

// ---------------------------------------------------------------------------
// Gameplay — direct replacement for the TCP pipe
// ---------------------------------------------------------------------------

void KafkaNet::SubscribeGameplay(const std::string& gameCode,
                                  const std::string& playerId) {
    UnsubscribeGameplay(); // avoid leaking a consumer if already subscribed to a prior session
    std::string err;
    auto* cconf = MakeBaseConf(cfg_, err, static_cast<RdKafka::EventCb*>(eventCb_));
    if (!cconf) { lastError_ = err; NetLog::Write("SubscribeGameplay FAILED (conf): " + err); return; }

    // Unique consumer group per player per game — both players receive all messages.
    std::string groupId = "kafkatanx-" + gameCode + "-" + playerId;
    cconf->set("group.id",             groupId,    err);
    cconf->set("auto.offset.reset",    "earliest", err);
    cconf->set("enable.auto.commit",   "true",     err);
    // 30s rather than the default 10s: this consumer is alone in its group
    // (unique per session), so there's no rebalance-contention downside to
    // giving it more slack — only upside, since a brief wifi/NAT blip on
    // either player's side shouldn't evict group membership and force a
    // rejoin mid-match. The app's own PING/silence timeout (NetUpdate() in
    // kafkatanx.cpp) is the real liveness signal for "opponent is gone";
    // this just stops the consumer's own plumbing from adding false drops
    // on top of that.
    cconf->set("session.timeout.ms",   "30000",    err);
    cconf->set("rebalance_cb", static_cast<RdKafka::RebalanceCb*>(rebalanceCb_), err);

    auto* cons = RdKafka::KafkaConsumer::create(cconf, err);
    delete cconf;
    if (!cons) { lastError_ = "Consumer create: " + err; NetLog::Write("SubscribeGameplay FAILED (create): " + err); return; }

    cons->subscribe({ std::string(KTopic::GAMEPLAY) });
    consumer_ = cons;
    NetLog::Write("Subscribed gameCode=" + gameCode + " group=" + groupId);
}

void KafkaNet::UnsubscribeGameplay() {
    if (!consumer_) return;
    auto* c = static_cast<RdKafka::KafkaConsumer*>(consumer_);
    c->close();
    delete c;
    consumer_ = nullptr;
    NetLog::Write("Unsubscribed gameplay consumer");
}

bool KafkaNet::SendGameplay(const std::string& gameCode, int32_t msgType,
                             const std::vector<uint8_t>& payload) {
    if (!producer_) return false;
    auto* p = static_cast<RdKafka::Producer*>(producer_);

    // Encode GameplayMessage as Avro binary
    AvroWriter w;
    w.WriteString(gameCode);
    w.WriteString(cfg_.playerId);
    w.WriteLong(++sendSeq_);
    w.WriteEnum(msgType);
    w.WriteBytes(payload);

    auto wire = AvroWire::Wrap(cfg_.schemaIdGameplay, w.Data());

    auto rc = p->produce(KTopic::GAMEPLAY,
                         RdKafka::Topic::PARTITION_UA,
                         RdKafka::Producer::RK_MSG_COPY,
                         wire.data(), wire.size(),
                         gameCode.c_str(), gameCode.size(),
                         0, nullptr);
    p->poll(0);

    if (rc != RdKafka::ERR_NO_ERROR) {
        lastError_ = RdKafka::err2str(rc);
        NetLog::Write("SendGameplay FAILED type=" + std::to_string(msgType) +
                      " gameCode=" + gameCode + ": " + RdKafka::err2str(rc));
        return false;
    }
    return true;
}

bool KafkaNet::PollGameplay(KafkaMsg& out, int timeoutMs) {
    if (!consumer_) return false;
    auto* c = static_cast<RdKafka::KafkaConsumer*>(consumer_);

    auto* msg = c->consume(timeoutMs);
    if (!msg) return false;

    if (msg->err()) {
        if (msg->err() != RdKafka::ERR__TIMED_OUT) {
            lastError_ = msg->errstr();
            NetLog::Write("PollGameplay consume error: " + msg->errstr());
        }
        delete msg;
        return false;
    }

    int32_t        schemaId;
    const uint8_t* payload;
    size_t         payloadLen;

    bool unwrapped = AvroWire::Unwrap(
        static_cast<const uint8_t*>(msg->payload()), msg->len(),
        schemaId, payload, payloadLen);

    bool decoded = false;
    if (unwrapped && schemaId == cfg_.schemaIdGameplay) {
        AvroReader r(payload, payloadLen);
        out.gameCode       = r.ReadString();
        out.senderPlayerId = r.ReadString();
        out.sequence       = r.ReadLong();
        out.messageType    = r.ReadEnum();
        out.payload        = r.ReadBytes();
        decoded = r.Ok();
    }
    // Note: a schema ID mismatch here is NOT surfaced as an error. Every fresh
    // session subscribes from `earliest` on the shared kafkatanx-gameplay topic
    // (24h retention), so a brand-new HOST with no opponent yet routinely
    // replays old messages from other sessions — including ones tagged with a
    // since-superseded schema ID after any schema bump. That's expected
    // background noise, not a live problem, and there's no way to tell the two
    // apart here (a mismatched schema can't even be decoded far enough to check
    // whether it belongs to this gameCode). A genuinely unreachable/incompatible
    // opponent is already caught by the heartbeat/disconnect-timeout mechanism
    // in NetUpdate(), which is a much clearer signal than a schema ID number.

    delete msg;
    return decoded;
}

// ---------------------------------------------------------------------------
// Session management (kafkatanx-sessions, compacted)
// ---------------------------------------------------------------------------

static bool ProduceSession(RdKafka::Producer* p, const KafkaConfig& cfg,
                            const std::string& gameCode, const std::string& status) {
    AvroWriter w;
    w.WriteString(gameCode);
    w.WriteString(cfg.playerId);
    w.WriteString(cfg.playerName);
    w.WriteEnum(status == "WAITING"   ? 0 :
                status == "ACTIVE"    ? 1 :
                status == "COMPLETE"  ? 2 : 3);  // 3 = ABANDONED

    using namespace std::chrono;
    int64_t now = duration_cast<milliseconds>(
                      system_clock::now().time_since_epoch()).count();
    w.WriteLong(now);
    w.WriteString("{}");  // settings_json placeholder — populated in MATCH_START

    auto wire = AvroWire::Wrap(cfg.schemaIdSession, w.Data());
    auto rc   = p->produce(KTopic::SESSIONS,
                           RdKafka::Topic::PARTITION_UA,
                           RdKafka::Producer::RK_MSG_COPY,
                           wire.data(), wire.size(),
                           gameCode.c_str(), gameCode.size(),
                           0, nullptr);
    p->poll(0);
    return rc == RdKafka::ERR_NO_ERROR;
}

bool KafkaNet::PublishSessionWaiting(const std::string& gameCode) {
    if (!producer_) return false;
    return ProduceSession(static_cast<RdKafka::Producer*>(producer_),
                          cfg_, gameCode, "WAITING");
}

bool KafkaNet::PublishSessionActive(const std::string& gameCode) {
    if (!producer_) return false;
    return ProduceSession(static_cast<RdKafka::Producer*>(producer_),
                          cfg_, gameCode, "ACTIVE");
}

bool KafkaNet::PublishSessionComplete(const std::string& gameCode) {
    if (!producer_) return false;
    return ProduceSession(static_cast<RdKafka::Producer*>(producer_),
                          cfg_, gameCode, "COMPLETE");
}

bool KafkaNet::PublishSessionAbandoned(const std::string& gameCode) {
    if (!producer_) return false;
    return ProduceSession(static_cast<RdKafka::Producer*>(producer_),
                          cfg_, gameCode, "ABANDONED");
}

// ---------------------------------------------------------------------------
// Player identity (kafkatanx-players, compacted)
// ---------------------------------------------------------------------------

bool KafkaNet::PublishPlayerProfile() {
    if (!producer_) return false;
    auto* p = static_cast<RdKafka::Producer*>(producer_);

    using namespace std::chrono;
    int64_t now = duration_cast<milliseconds>(
                      system_clock::now().time_since_epoch()).count();

    AvroWriter w;
    w.WriteString(cfg_.playerId);
    w.WriteString(cfg_.playerName);
    w.WriteLong(now);   // first_seen — Flink keeps the earliest; this simplifies the write
    w.WriteLong(now);   // last_seen
    w.WriteString("1.0.0");  // client_version

    auto wire = AvroWire::Wrap(cfg_.schemaIdPlayer, w.Data());
    auto rc   = p->produce(KTopic::PLAYERS,
                           RdKafka::Topic::PARTITION_UA,
                           RdKafka::Producer::RK_MSG_COPY,
                           wire.data(), wire.size(),
                           cfg_.playerId.c_str(), cfg_.playerId.size(),
                           0, nullptr);
    p->poll(0);
    return rc == RdKafka::ERR_NO_ERROR;
}

// ---------------------------------------------------------------------------
// Analytics — HOST only
// ---------------------------------------------------------------------------

bool KafkaNet::PublishShot(const ShotEventData& ev) {
    if (!producer_) return false;
    auto* p = static_cast<RdKafka::Producer*>(producer_);

    AvroWriter w;
    w.WriteString(ev.gameCode);
    w.WriteInt(ev.round);
    w.WriteInt(ev.turn);
    w.WriteString(ev.shooterPlayerId);
    w.WriteString(ev.shooterName);
    w.WriteString(ev.targetPlayerId);
    w.WriteString(ev.targetName);
    w.WriteString(ev.weapon);
    w.WriteFloat(ev.angle);
    w.WriteFloat(ev.power);
    w.WriteFloat(ev.windSpeed);
    w.WriteString(ev.gravitySetting);
    w.WriteString(ev.landscapeSetting);
    w.WriteBool(ev.nightMode);
    w.WriteBool(ev.hit);
    w.WriteInt(ev.damageDealt);
    w.WriteInt(ev.targetHpBefore);
    w.WriteInt(ev.targetHpAfter);
    w.WriteFloat(ev.craterX);
    w.WriteFloat(ev.craterY);
    w.WriteLong(ev.shotAt);

    auto wire = AvroWire::Wrap(cfg_.schemaIdShot, w.Data());
    std::string key = ev.gameCode + "-" + std::to_string(ev.round)
                                  + "-" + std::to_string(ev.turn);
    p->produce(KTopic::SHOTS, RdKafka::Topic::PARTITION_UA,
               RdKafka::Producer::RK_MSG_COPY,
               wire.data(), wire.size(),
               key.c_str(), key.size(), 0, nullptr);
    p->poll(0);
    return true;
}

bool KafkaNet::PublishRound(const RoundEventData& ev) {
    if (!producer_) return false;
    auto* p = static_cast<RdKafka::Producer*>(producer_);

    AvroWriter w;
    w.WriteString(ev.gameCode);
    w.WriteInt(ev.roundNumber);
    w.WriteNullableString(ev.winnerPlayerId, ev.winnerPlayerId.empty());
    w.WriteNullableString(ev.winnerName,     ev.winnerName.empty());
    w.WriteNullableString(ev.drawReason,     ev.drawReason.empty());
    w.WriteInt(ev.turnsTaken);
    w.WriteInt(ev.shotsFired);
    w.WriteFloat(ev.durationSeconds);
    w.WriteLong(ev.endedAt);

    auto wire = AvroWire::Wrap(cfg_.schemaIdRound, w.Data());
    std::string key = ev.gameCode + "-" + std::to_string(ev.roundNumber);
    p->produce(KTopic::ROUNDS, RdKafka::Topic::PARTITION_UA,
               RdKafka::Producer::RK_MSG_COPY,
               wire.data(), wire.size(),
               key.c_str(), key.size(), 0, nullptr);
    p->poll(0);
    return true;
}

bool KafkaNet::PublishGame(const GameEventData& ev) {
    if (!producer_) return false;
    auto* p = static_cast<RdKafka::Producer*>(producer_);

    AvroWriter w;
    w.WriteString(ev.gameCode);
    w.WriteString(ev.hostPlayerId);
    w.WriteString(ev.hostName);
    w.WriteString(ev.clientPlayerId);
    w.WriteString(ev.clientName);
    w.WriteNullableString(ev.winnerPlayerId, ev.winnerPlayerId.empty());
    w.WriteNullableString(ev.winnerName,     ev.winnerName.empty());
    w.WriteInt(ev.roundsPlayed);
    w.WriteInt(ev.totalTurns);
    w.WriteString(ev.windSetting);
    w.WriteString(ev.gravitySetting);
    w.WriteString(ev.landscapeSetting);
    w.WriteBool(ev.nightMode);
    w.WriteInt(ev.roundsToWin);
    w.WriteInt(ev.startingHp);
    w.WriteLong(ev.startedAt);
    w.WriteLong(ev.endedAt);
    w.WriteFloat(ev.durationSeconds);
    w.WriteInt(ev.hostMovesTotal);
    w.WriteInt(ev.hostTurns);
    w.WriteInt(ev.clientMovesTotal);
    w.WriteInt(ev.clientTurns);

    auto wire = AvroWire::Wrap(cfg_.schemaIdGame, w.Data());
    p->produce(KTopic::GAMES, RdKafka::Topic::PARTITION_UA,
               RdKafka::Producer::RK_MSG_COPY,
               wire.data(), wire.size(),
               ev.gameCode.c_str(), ev.gameCode.size(), 0, nullptr);
    p->poll(0);
    return true;
}
