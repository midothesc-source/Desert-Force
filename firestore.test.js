const {
  initializeTestEnvironment,
  assertFails,
  assertSucceeds,
} = require("@firebase/rules-unit-testing");
const { test, before, after, beforeEach } = require("node:test");
const fs = require("node:fs");

let testEnv;
const PROJECT_ID = process.env.GCP_PROJECT || "demo-no-project";
const ALICE_UID = "alice_123";
const BOB_UID = "bob_456";

const [emulatorHost, emulatorPortStr] = (process.env.FIRESTORE_EMULATOR_HOST || "127.0.0.1:8085").split(":");
const emulatorPort = parseInt(emulatorPortStr, 10);

before(async () => {
  const rules = fs.readFileSync("./firestore.rules", "utf8");
  testEnv = await initializeTestEnvironment({
    projectId: PROJECT_ID,
    firestore: {
      rules,
      host: emulatorHost,
      port: emulatorPort,
    },
  });
});

after(async () => {
  if (testEnv) {
    await testEnv.cleanup();
  }
});

beforeEach(async () => {
  if (testEnv) {
    await testEnv.clearFirestore();
  }
});

test("Unauthenticated user: cannot read messages, intel_docs, or advocacy_campaigns", async () => {
  const unauthDb = testEnv.unauthenticatedContext().firestore();
  await assertFails(unauthDb.collection("messages").get());
  await assertFails(unauthDb.collection("intel_docs").get());
  await assertFails(unauthDb.collection("advocacy_campaigns").get());
});

test("Authenticated user: cannot read another user's message", async () => {
  await testEnv.withSecurityRulesDisabled(async (context) => {
    await context.firestore().collection("messages").doc("bob_msg").set({
      userId: BOB_UID,
      channelId: "global-command",
      senderName: "Bob",
      senderRole: "Operator",
      content: "Tactical update",
      languageCode: "en",
      isAiGenerated: false,
      priority: "STANDARD",
      createdAt: new Date(),
      updatedAt: new Date(),
    });
  });

  const aliceDb = testEnv.authenticatedContext(ALICE_UID).firestore();
  await assertFails(aliceDb.collection("messages").doc("bob_msg").get());
});

test("Authenticated user: can query their own messages with matching userId filter", async () => {
  await testEnv.withSecurityRulesDisabled(async (context) => {
    await context.firestore().collection("messages").doc("alice_msg").set({
      userId: ALICE_UID,
      channelId: "global-command",
      senderName: "Alice",
      senderRole: "Commander",
      content: "Desert Force online",
      languageCode: "en",
      isAiGenerated: false,
      priority: "HIGH",
      createdAt: new Date(),
      updatedAt: new Date(),
    });
  });

  const aliceDb = testEnv.authenticatedContext(ALICE_UID).firestore();
  await assertSucceeds(
    aliceDb.collection("messages").where("userId", "==", ALICE_UID).get()
  );
});

test("Authenticated user: fails query without userId filter (rules are not filters)", async () => {
  const aliceDb = testEnv.authenticatedContext(ALICE_UID).firestore();
  await assertFails(aliceDb.collection("messages").get());
});

test("Authenticated user: rejects shadow update with ghost field on intel_docs", async () => {
  const now = new Date();
  await testEnv.withSecurityRulesDisabled(async (context) => {
    await context.firestore().collection("intel_docs").doc("doc_1").set({
      userId: ALICE_UID,
      title: "Desert Solar Grids",
      category: "EDUCATION",
      summary: "Field manual for solar microgrids.",
      fileFormat: "PDF",
      readTimeMinutes: 10,
      bookmarked: false,
      createdAt: now,
      updatedAt: now,
    });
  });

  const aliceDb = testEnv.authenticatedContext(ALICE_UID).firestore();
  await assertFails(
    aliceDb.collection("intel_docs").doc("doc_1").update({
      bookmarked: true,
      isVerifiedAdmin: true,
      updatedAt: new Date(),
    })
  );
});
