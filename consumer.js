const { Kafka } = require("kafkajs");

function now() {
  return new Date().toISOString();
}

/**
 * Ex:
 * bank.victor99dev.Domain.Events.AccountCreatedDomainEvent
 * -> AccountCreated
 */
function normalizeEventName(type) {
  if (!type || typeof type !== "string") return "Unknown";
  const last = type.split(".").pop() || type;      // AccountCreatedDomainEvent
  return last.replace("DomainEvent", "");          // AccountCreated
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

/**
 * Teu evento tem 2 níveis:
 * root.payload.body é onde está o "snapshot" de negócio
 */
function extractBody(root) {
  if (!root || typeof root !== "object") return null;

  const payload = root.payload;
  if (payload && typeof payload === "object") {
    if (payload.body && typeof payload.body === "object") return payload.body;
    return payload; // fallback (caso não venha body)
  }

  return null;
}

function printEvent(evt) {
  console.log("\n==================================================");
  console.log(`Received at: ${now()}`);
  console.log(`Topic: ${evt.topic}`);
  console.log(`Partition: ${evt.partition}`);
  console.log(`Offset: ${evt.offset}`);
  console.log(`MessageKey: ${evt.messageKey ?? "n/a"}`);
  console.log("---");
  console.log(`EventType: ${evt.eventType}`);
  console.log(`EventName: ${evt.eventName}`);
  console.log(`EventId: ${evt.eventId ?? "n/a"}`);
  console.log(`OccurredOnUtc: ${evt.occurredOnUtc ?? "n/a"}`);
  console.log(`AggregateId: ${evt.aggregateId ?? "n/a"}`);
  console.log(`Key: ${evt.key ?? "n/a"}`);
  console.log(`CorrelationId: ${evt.correlationId ?? "n/a"}`);
  console.log("Body:");
  console.dir(evt.body, { depth: null });
  console.log("==================================================\n");
}

function react(evt) {
  switch (evt.eventName) {
    case "AccountCreated":
      console.log("[ACTION] Fraud: schedule initial risk assessment");
      console.log("[ACTION] CRM: create customer profile");
      console.log("[ACTION] Notifications: enqueue welcome notification");
      break;

    case "AccountActivated":
      console.log("[ACTION] Cards: enable card issuance");
      console.log("[ACTION] CRM: mark account active");
      break;

    case "AccountDeactivated":
      console.log("[ACTION] Cards: block cards");
      console.log("[ACTION] Fraud: increase monitoring");
      break;

    case "AccountDeleted":
      console.log("[ACTION] CRM: mark account deleted");
      console.log("[ACTION] Analytics: emit deletion metric");
      break;

    case "AccountRestored":
      console.log("[ACTION] CRM: restore account visibility");
      break;

    case "AccountCpfChanged":
      console.log("[ACTION] Downstream: update CPF references");
      break;

    case "AccountNameChanged":
      console.log("[ACTION] CRM: update customer name");
      break;

    case "AccountUpdated":
      console.log("[ACTION] CRM: sync account snapshot");
      break;

    default:
      console.log("[ACTION] No mapped reaction (audit only).");
      break;
  }

  console.log("");
}

async function start() {
  const kafka = new Kafka({
    clientId: "bank-api",
    brokers: ["localhost:9092"],
  });

  const consumer = kafka.consumer({ groupId: "bank-account-observers" });

  await consumer.connect();

  const topic = "bank.victor99dev.events";
  await consumer.subscribe({ topic, fromBeginning: true });

  console.log(`Consumer connected`);
  console.log(`Topic: ${topic}`);
  console.log(`GroupId: bank-account-observers`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString("utf8") || "";
      const messageKey = message.key?.toString("utf8") || null;

      const headers = {};
      if (message.headers) {
        for (const [k, v] of Object.entries(message.headers)) {
          headers[k] = v ? v.toString("utf8") : null;
        }
      }

      const root = safeJsonParse(raw);

      // Se não for JSON, imprime raw e sai
      if (!root) {
        console.log("\n--- RAW MESSAGE START ---");
        console.log("topic:", topic);
        console.log("partition:", partition);
        console.log("offset:", message.offset);
        console.log("key:", messageKey);
        console.log("headers:", headers);
        console.log("value(raw):", raw);
        console.log("--- RAW MESSAGE END ---\n");
        return;
      }

      const eventType = root.type ?? "Unknown";
      const eventName = normalizeEventName(eventType);

      // Preferir os campos do root; se faltar, cai pro payload
      const payload = root.payload ?? {};
      const evt = {
        topic,
        partition,
        offset: message.offset,
        messageKey,
        headers,

        eventType,
        eventName,

        eventId: root.eventId ?? payload.eventId ?? null,
        occurredOnUtc: root.occurredOnUtc ?? payload.occurredOnUtc ?? null,
        correlationId: root.correlationId ?? null,
        aggregateId: root.aggregateId ?? payload.aggregateId ?? null,
        key: root.key ?? payload.key ?? null,

        body: extractBody(root),
      };

      printEvent(evt);
      react(evt);
    },
  });
}

start().catch((err) => {
  console.error("Consumer crashed:", err);
  process.exit(1);
});
