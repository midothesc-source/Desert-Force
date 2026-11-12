package com.example.data.repository

import android.content.Context
import com.example.R
import com.example.data.model.AdvocacyCampaign
import com.example.data.model.IntelDocument
import com.example.data.model.TacticalMessage
import com.google.firebase.Firebase
import com.google.firebase.Timestamp
import com.google.firebase.auth.auth
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.FieldValue
import com.google.firebase.firestore.FirebaseFirestore
import com.google.firebase.firestore.FirebaseFirestoreException
import com.google.firebase.firestore.snapshots
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.tasks.await

class DesertForceRepository(private val db: FirebaseFirestore) {

    constructor(context: Context) : this(
        FirebaseFirestore.getInstance(
            context.applicationContext.getString(R.string.firestore_database_id)
        )
    )

    private val auth get() = Firebase.auth

    private fun requireUserId(): String {
        return auth.currentUser?.uid
            ?: throw IllegalStateException("User must be signed in with Google before accessing Firestore.")
    }

    // --- 1. TACTICAL MESSAGES ---

    fun observeMessages(channelId: String): Flow<List<TacticalMessage>> = flow {
        val uid = requireUserId()
        val path = "messages"
        emitAll(
            db.collection(path)
                .whereEqualTo("userId", uid)
                .snapshots()
                .map { snapshot ->
                    snapshot.documents.mapNotNull { doc ->
                        doc.toObject(
                            TacticalMessage::class.java,
                            DocumentSnapshot.ServerTimestampBehavior.ESTIMATE
                        )
                    }
                        .filter { it.channelId == channelId }
                        .sortedBy { (it.createdAt ?: Timestamp(0, 0)).seconds }
                }
                .catch { error ->
                    val firestoreError = generateSequence(error) { it.cause }
                        .filterIsInstance<FirebaseFirestoreException>()
                        .firstOrNull()
                    if (firestoreError == null && error is CancellationException) throw error
                    val exceptionToLog = firestoreError ?: (error as? Exception)
                    if (exceptionToLog != null && auth.currentUser?.uid == uid) {
                        handleFirestoreError(exceptionToLog, OperationType.LIST, path)
                    }
                    throw error
                }
        )
    }

    suspend fun getMessageById(messageId: String): Result<TacticalMessage> {
        val docRef = db.collection("messages").document(messageId)
        return try {
            val snap = docRef.get().await()
            val msg = snap.toObject(
                TacticalMessage::class.java,
                DocumentSnapshot.ServerTimestampBehavior.ESTIMATE
            ) ?: throw NoSuchElementException("Message not found")
            Result.success(msg)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.GET, docRef.path)
            Result.failure(e)
        }
    }

    suspend fun sendMessage(
        channelId: String,
        senderName: String,
        senderRole: String,
        content: String,
        languageCode: String = "en",
        isAiGenerated: Boolean = false,
        priority: String = "STANDARD",
        attachmentTitle: String? = null,
        attachmentType: String? = null
    ): Result<String> {
        val uid = requireUserId()
        val docRef = db.collection("messages").document()
        val payload = mutableMapOf<String, Any>(
            "userId" to uid,
            "channelId" to channelId.take(64),
            "senderName" to senderName.take(80).ifBlank { "Falcon Operator" },
            "senderRole" to senderRole.take(80).ifBlank { "Field Commander" },
            "content" to content.trim().take(4000),
            "languageCode" to languageCode.take(16).ifBlank { "en" },
            "isAiGenerated" to isAiGenerated,
            "priority" to if (priority in listOf("STANDARD", "HIGH", "FLASH")) priority else "STANDARD",
            "createdAt" to FieldValue.serverTimestamp(),
            "updatedAt" to FieldValue.serverTimestamp()
        )
        if (!attachmentTitle.isNullOrBlank()) {
            payload["attachmentTitle"] = attachmentTitle.take(200)
        }
        if (!attachmentType.isNullOrBlank()) {
            payload["attachmentType"] = attachmentType.take(32)
        }

        return try {
            docRef.set(payload).await()
            Result.success(docRef.id)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.CREATE, docRef.path)
            Result.failure(e)
        }
    }

    suspend fun updateMessageTranslation(
        messageId: String,
        translatedContent: String,
        translatedLang: String
    ): Result<Unit> {
        requireUserId()
        val docRef = db.collection("messages").document(messageId)
        val updates = mapOf(
            "translatedContent" to translatedContent.take(4000),
            "translatedLang" to translatedLang.take(16),
            "updatedAt" to FieldValue.serverTimestamp()
        )
        return try {
            docRef.update(updates).await()
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.UPDATE, docRef.path)
            Result.failure(e)
        }
    }

    suspend fun deleteMessage(messageId: String): Result<Unit> {
        requireUserId()
        val docRef = db.collection("messages").document(messageId)
        return try {
            docRef.delete().await()
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.DELETE, docRef.path)
            Result.failure(e)
        }
    }

    // --- 2. INTEL & EDUCATIONAL DOCUMENTS ---

    fun observeIntelDocs(): Flow<List<IntelDocument>> = flow {
        val uid = requireUserId()
        val path = "intel_docs"
        emitAll(
            db.collection(path)
                .whereEqualTo("userId", uid)
                .snapshots()
                .map { snapshot ->
                    snapshot.documents.mapNotNull { doc ->
                        doc.toObject(
                            IntelDocument::class.java,
                            DocumentSnapshot.ServerTimestampBehavior.ESTIMATE
                        )
                    }.sortedByDescending { (it.createdAt ?: Timestamp(0, 0)).seconds }
                }
                .catch { error ->
                    val firestoreError = generateSequence(error) { it.cause }
                        .filterIsInstance<FirebaseFirestoreException>()
                        .firstOrNull()
                    if (firestoreError == null && error is CancellationException) throw error
                    val exceptionToLog = firestoreError ?: (error as? Exception)
                    if (exceptionToLog != null && auth.currentUser?.uid == uid) {
                        handleFirestoreError(exceptionToLog, OperationType.LIST, path)
                    }
                    throw error
                }
        )
    }

    suspend fun createIntelDocument(
        title: String,
        category: String,
        summary: String,
        fileFormat: String,
        readTimeMinutes: Long,
        bookmarked: Boolean = false
    ): Result<String> {
        val uid = requireUserId()
        val docRef = db.collection("intel_docs").document()
        val payload = mapOf(
            "userId" to uid,
            "title" to title.trim().take(160),
            "category" to if (category in listOf("EDUCATION", "HUMANITARIAN", "PROTOCOL", "MEDIA")) category else "EDUCATION",
            "summary" to summary.trim().take(4000),
            "fileFormat" to if (fileFormat in listOf("PDF", "AUDIO", "VIDEO", "BRIEF")) fileFormat else "PDF",
            "readTimeMinutes" to readTimeMinutes.coerceIn(1L, 240L),
            "bookmarked" to bookmarked,
            "createdAt" to FieldValue.serverTimestamp(),
            "updatedAt" to FieldValue.serverTimestamp()
        )
        return try {
            docRef.set(payload).await()
            Result.success(docRef.id)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.CREATE, docRef.path)
            Result.failure(e)
        }
    }

    suspend fun toggleDocumentBookmark(docId: String, bookmarked: Boolean): Result<Unit> {
        requireUserId()
        val docRef = db.collection("intel_docs").document(docId)
        val updates = mapOf(
            "bookmarked" to bookmarked,
            "updatedAt" to FieldValue.serverTimestamp()
        )
        return try {
            docRef.update(updates).await()
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.UPDATE, docRef.path)
            Result.failure(e)
        }
    }

    // --- 3. HUMANITARIAN ADVOCACY CAMPAIGNS ---

    fun observeAdvocacyCampaigns(): Flow<List<AdvocacyCampaign>> = flow {
        val uid = requireUserId()
        val path = "advocacy_campaigns"
        emitAll(
            db.collection(path)
                .whereEqualTo("userId", uid)
                .snapshots()
                .map { snapshot ->
                    snapshot.documents.mapNotNull { doc ->
                        doc.toObject(
                            AdvocacyCampaign::class.java,
                            DocumentSnapshot.ServerTimestampBehavior.ESTIMATE
                        )
                    }.sortedByDescending { it.supportersCount }
                }
                .catch { error ->
                    val firestoreError = generateSequence(error) { it.cause }
                        .filterIsInstance<FirebaseFirestoreException>()
                        .firstOrNull()
                    if (firestoreError == null && error is CancellationException) throw error
                    val exceptionToLog = firestoreError ?: (error as? Exception)
                    if (exceptionToLog != null && auth.currentUser?.uid == uid) {
                        handleFirestoreError(exceptionToLog, OperationType.LIST, path)
                    }
                    throw error
                }
        )
    }

    suspend fun createAdvocacyCampaign(
        title: String,
        region: String,
        causeCategory: String,
        impactSummary: String,
        supportersCount: Long,
        pledgedSupport: Boolean = false,
        urgencyLevel: String = "ACTIVE"
    ): Result<String> {
        val uid = requireUserId()
        val docRef = db.collection("advocacy_campaigns").document()
        val validCategory = if (causeCategory in listOf(
                "WATER_SECURITY", "MEDICAL_RELIEF", "EDUCATION_ACCESS", "CLIMATE_RESILIENCE"
            )
        ) causeCategory else "WATER_SECURITY"
        val validUrgency = if (urgencyLevel in listOf("CRITICAL", "ACTIVE", "SUSTAINED")) urgencyLevel else "ACTIVE"

        val payload = mapOf(
            "userId" to uid,
            "title" to title.trim().take(160),
            "region" to region.trim().take(100),
            "causeCategory" to validCategory,
            "impactSummary" to impactSummary.trim().take(4000),
            "supportersCount" to supportersCount.coerceIn(0L, 100_000_000L),
            "pledgedSupport" to pledgedSupport,
            "urgencyLevel" to validUrgency,
            "createdAt" to FieldValue.serverTimestamp(),
            "updatedAt" to FieldValue.serverTimestamp()
        )
        return try {
            docRef.set(payload).await()
            Result.success(docRef.id)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.CREATE, docRef.path)
            Result.failure(e)
        }
    }

    suspend fun pledgeCampaignSupport(
        campaignId: String,
        currentSupporters: Long,
        pledged: Boolean
    ): Result<Unit> {
        requireUserId()
        val docRef = db.collection("advocacy_campaigns").document(campaignId)
        val newSupporters = if (pledged) (currentSupporters + 1L) else (currentSupporters - 1L).coerceAtLeast(0L)
        val updates = mapOf(
            "pledgedSupport" to pledged,
            "supportersCount" to newSupporters,
            "updatedAt" to FieldValue.serverTimestamp()
        )
        return try {
            docRef.update(updates).await()
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            handleFirestoreError(e, OperationType.UPDATE, docRef.path)
            Result.failure(e)
        }
    }

    // --- 4. INITIAL TACTICAL WORKSPACE SEEDING ---

    suspend fun seedInitialWorkspaceIfEmpty() {
        val uid = try {
            requireUserId()
        } catch (e: Exception) {
            return
        }

        try {
            val existingMsgs = db.collection("messages")
                .whereEqualTo("userId", uid)
                .limit(1)
                .get()
                .await()
            if (existingMsgs.isEmpty) {
                sendMessage(
                    channelId = "global-command",
                    senderName = "Falcon-1 Command",
                    senderRole = "Strategic Operations Director",
                    content = "Desert Force Relay Node active. End-to-end cryptographic mesh verified across 14 regional corridors. Sub-millisecond Firestore sync engaged.",
                    languageCode = "en",
                    isAiGenerated = false,
                    priority = "HIGH",
                    attachmentTitle = "Mesh_Topology_v4.2.pdf",
                    attachmentType = "PDF"
                )
                sendMessage(
                    channelId = "global-command",
                    senderName = "Dr. Layla Al-Mansoor",
                    senderRole = "Humanitarian Logistics Lead",
                    content = "القافلة الطبية وصلت إلى القطاع الجنوبي بنجاح. جميع وحدات الطاقة الشمسية وتنقية المياه تعمل بكفاءة 100٪.",
                    languageCode = "ar",
                    isAiGenerated = false,
                    priority = "FLASH",
                    attachmentTitle = "Solar_Water_Telemetry.brief",
                    attachmentType = "BRIEF"
                )
                sendMessage(
                    channelId = "humanitarian-relief",
                    senderName = "Oasis Relief Dispatch",
                    senderRole = "Field Medical Coordinator",
                    content = "Mobile satellite clinics deployed in Sector 7. Requesting multilingual educational packets in Arabic, French, and Spanish for community health workers.",
                    languageCode = "en",
                    isAiGenerated = false,
                    priority = "HIGH"
                )
                sendMessage(
                    channelId = "arabic-dispatch",
                    senderName = "القائد طارق الصحراء",
                    senderRole = "مدير الاتصالات الميدانية",
                    content = "مرحباً بكم في شبكة قوة الصحراء للاتصالات الفورية فائقة السرعة. يمكنك تبديل لغة التطبيق إلى أي لغة في العالم مع دعم كامل للكتابة من اليمين إلى اليسار.",
                    languageCode = "ar",
                    isAiGenerated = false,
                    priority = "STANDARD"
                )
            }

            val existingDocs = db.collection("intel_docs")
                .whereEqualTo("userId", uid)
                .limit(1)
                .get()
                .await()
            if (existingDocs.isEmpty) {
                createIntelDocument(
                    title = "Desert Force Zero-Trust Encryption & Mesh Architecture",
                    category = "PROTOCOL",
                    summary = "Technical blueprint detailing sub-millisecond Firestore synchronization, offline resilience, and multi-tenant zero-trust ABAC security rules.",
                    fileFormat = "PDF",
                    readTimeMinutes = 8L,
                    bookmarked = true
                )
                createIntelDocument(
                    title = "Solar Desalination & Autonomous Water Microgrids",
                    category = "EDUCATION",
                    summary = "Educational field guide for deploying modular photovoltaic water purification units in arid and conflict-impacted corridors.",
                    fileFormat = "BRIEF",
                    readTimeMinutes = 12L,
                    bookmarked = true
                )
                createIntelDocument(
                    title = "Multilingual Emergency Triage Audio Protocol (AR/EN/FR/ES)",
                    category = "HUMANITARIAN",
                    summary = "Rapid field communication handbook and phonetic audio transcripts for cross-border medical and humanitarian relief teams.",
                    fileFormat = "AUDIO",
                    readTimeMinutes = 6L,
                    bookmarked = false
                )
            }

            val existingCampaigns = db.collection("advocacy_campaigns")
                .whereEqualTo("userId", uid)
                .limit(1)
                .get()
                .await()
            if (existingCampaigns.isEmpty) {
                createAdvocacyCampaign(
                    title = "Clean Oasis Initiative: Solar Water Wells for Arid Communities",
                    region = "MENA & Sahel Corridor",
                    causeCategory = "WATER_SECURITY",
                    impactSummary = "Deploying 120 solar-powered atmospheric and deep-aquifer water stations serving 480,000 displaced families with real-time telemetry transparency.",
                    supportersCount = 14820L,
                    pledgedSupport = true,
                    urgencyLevel = "CRITICAL"
                )
                createAdvocacyCampaign(
                    title = "Connected Classrooms: Satellite Digital Libraries for Youth",
                    region = "Global Humanitarian Zones",
                    causeCategory = "EDUCATION_ACCESS",
                    impactSummary = "Delivering offline-first multilingual STEM curricula, solar tablets, and AI-translated educational archives to 350 remote learning hubs.",
                    supportersCount = 9430L,
                    pledgedSupport = false,
                    urgencyLevel = "ACTIVE"
                )
                createAdvocacyCampaign(
                    title = "Mobile Trauma & Telemedicine Airbridge",
                    region = "Red Sea & Levantine Relief Zones",
                    causeCategory = "MEDICAL_RELIEF",
                    impactSummary = "Coordinating emergency medical supply corridors and real-time AI-translated specialist consultations for frontline field hospitals.",
                    supportersCount = 21950L,
                    pledgedSupport = false,
                    urgencyLevel = "CRITICAL"
                )
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Logged inside individual methods
        }
    }
}
