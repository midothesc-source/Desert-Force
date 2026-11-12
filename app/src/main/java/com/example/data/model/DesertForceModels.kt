package com.example.data.model

import com.google.firebase.Timestamp
import com.google.firebase.firestore.DocumentId
import com.google.firebase.firestore.IgnoreExtraProperties
import com.google.firebase.firestore.ServerTimestamp

@IgnoreExtraProperties
data class TacticalMessage(
    @DocumentId val id: String = "",
    val userId: String = "",
    val channelId: String = "global-command",
    val senderName: String = "",
    val senderRole: String = "",
    val content: String = "",
    val languageCode: String = "en",
    val isAiGenerated: Boolean = false,
    val priority: String = "STANDARD",
    val translatedContent: String? = null,
    val translatedLang: String? = null,
    val attachmentTitle: String? = null,
    val attachmentType: String? = null,
    @ServerTimestamp val createdAt: Timestamp? = null,
    @ServerTimestamp val updatedAt: Timestamp? = null
)

@IgnoreExtraProperties
data class IntelDocument(
    @DocumentId val id: String = "",
    val userId: String = "",
    val title: String = "",
    val category: String = "EDUCATION",
    val summary: String = "",
    val fileFormat: String = "PDF",
    val readTimeMinutes: Long = 5L,
    val bookmarked: Boolean = false,
    @ServerTimestamp val createdAt: Timestamp? = null,
    @ServerTimestamp val updatedAt: Timestamp? = null
)

@IgnoreExtraProperties
data class AdvocacyCampaign(
    @DocumentId val id: String = "",
    val userId: String = "",
    val title: String = "",
    val region: String = "",
    val causeCategory: String = "WATER_SECURITY",
    val impactSummary: String = "",
    val supportersCount: Long = 0L,
    val pledgedSupport: Boolean = false,
    val urgencyLevel: String = "ACTIVE",
    @ServerTimestamp val createdAt: Timestamp? = null,
    @ServerTimestamp val updatedAt: Timestamp? = null
)

data class TacticalChannel(
    val id: String,
    val code: String,
    val nameEn: String,
    val nameAr: String,
    val nameFr: String,
    val nameEs: String,
    val subtitleEn: String,
    val subtitleAr: String,
    val encryptionBadge: String,
    val activeNodes: Int
)
