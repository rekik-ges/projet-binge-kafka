package fr.esgi.kafka.binge;

import fr.esgi.kafka.binge.common.JsonSerdes;
import fr.esgi.kafka.binge.model.PlaybackEvent;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * Construisez ici la topologie du projet BINGE.
 * Chaque ticket du backlog (README, section Backlog) correspond a un
 * bloc a implementer ci-dessous. Rien n'est code pour vous : les
 * commentaires rappellent seulement l'objectif et les APIs candidates.
 */
public final class BingeTopology {

    private static final Set<String> VALID_EVENT_TYPES =
            Set.of("PLAY", "PAUSE", "HEARTBEAT", "BUFFERING", "STOP");
    private static final Set<String> VALID_DEVICES =
            Set.of("TV", "MOBILE", "WEB", "CONSOLE");
    private static final Set<String> VALID_REGIONS =
            Set.of("EU-FR", "EU-DE", "EU-ES", "US-EAST", "US-WEST", "APAC");

    private static final String DEDUP_STORE_NAME = "binge-unique-play-events-store";
    private static final String VIEWS_STORE_NAME = "binge-views-by-title-store";

    private BingeTopology() {
    }

    public static void build(StreamsBuilder builder) {

        // Store de dedoublonnage (BINGE-2) : garde le dernier event_id vu,
        // pour detecter les doublons exacts avant de compter les vues.
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DEDUP_STORE_NAME),
                Serdes.String(), Serdes.Long()));

        // Flux brut : la valeur est du JSON... quand tout va bien.
        KStream<String, String> raw = builder.stream(
                Topics.PLAYBACK_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Routage valide / invalide (split/branch)
        // On valide sur le JSON brut (pas encore parse) pour pouvoir garder
        // le texte original si le message part en DLQ (tache suivante).
        // isValid() regroupe : parsing sur (poison pill) + les 3 validations
        // des taches precedentes (champs requis, types/bornes, enums).
        Map<String, KStream<String, String>> routed = raw.split(Named.as("route-"))
                .branch((key, value) -> isValid(value), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        KStream<String, String> validRaw = routed.get("route-valid");
        KStream<String, String> invalidRaw = routed.get("route-invalid");

        // La branche valide peut maintenant etre parsee sans risque : le
        // predicat isValid() a deja garanti un JSON lisible et conforme.
        KStream<String, PlaybackEvent> validEvents = validRaw.mapValues(
                value -> JsonSerdes.parseOrNull(value, PlaybackEvent.class));

        // Construction du message DLQ (reason + raw)
        // On reproduit les memes verifications que isValid(), mais ici pour
        // en extraire une raison lisible plutot qu'un simple booleen.
        // L'enveloppe garde le JSON original intact (raw) a cote de reason.
        KStream<String, String> dlqMessages = invalidRaw.mapValues(
                value -> JsonSerdes.toJson(new DlqEnvelope(rejectReason(value), value)));
        dlqMessages.to(Topics.DLQ);

        // -----------------------------------------------------------------
        // BINGE-1 - Ingestion fiable : socle termine (parsing, validation,
        //   routage, DLQ motivee). validEvents alimente les tickets suivants.
        // -----------------------------------------------------------------

        // BINGE-2 - Compteur de vues par contenu
        // Ne garder que les PLAY (seul evenement qui compte comme une vue).
        KStream<String, PlaybackEvent> playEvents = validEvents.filter(
                (key, event) -> "PLAY".equals(event.eventType()));

        // Dedoublonnage par event_id (state store DEDUP_STORE_NAME) : un
        // event_id deja vu est ignore, sinon il est enregistre et transmis.
        KStream<String, PlaybackEvent> uniquePlayEvents = playEvents.process(
                DedupProcessor::new, Named.as("dedup-play-events"), DEDUP_STORE_NAME);

        // Le comptage se fait par contenu, pas par utilisateur : on repasse
        // la cle de user_id (cle d'origine du topic) a content_id.
        KTable<String, Long> viewsByTitle = uniquePlayEvents
                .selectKey((key, event) -> event.contentId())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.of(PlaybackEvent.class)))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(VIEWS_STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        viewsByTitle.toStream()
                .mapValues((contentId, count) -> JsonSerdes.toJson(new ViewsByTitle(contentId, count)))
                .to(Topics.VIEWS_BY_TITLE);

        // BINGE-3 - Top tendances par genre (fenetres + jointure catalogue)
        //                                               -> Topics.TRENDING
        // BINGE-4 - Alerte qualite "buffering storm"    -> Topics.ALERTS_QOE
        // BINGE-5 - Sessions utilisateur (session windows) -> Topics.SESSIONS
        // BINGE-6 (bonus) - API REST Interactive Queries (cf. README)
    }

    // Comptage de vues - forme de l'enveloppe {"contentId":..., "count":...}
    private record ViewsByTitle(String contentId, long count) {
    }

    // Dedoublonnage par event_id (BINGE-2)
    // Un event_id deja present dans le store est un doublon exact : on ne
    // le transmet pas (return sans forward). Sinon on l'enregistre (valeur
    // = timestamp de traitement, non relu pour l'instant) et on le transmet.
    private static class DedupProcessor implements Processor<String, PlaybackEvent, String, PlaybackEvent> {
        private KeyValueStore<String, Long> store;
        private ProcessorContext<String, PlaybackEvent> context;

        @Override
        public void init(ProcessorContext<String, PlaybackEvent> context) {
            this.context = context;
            this.store = context.getStateStore(DEDUP_STORE_NAME);
        }

        @Override
        public void process(Record<String, PlaybackEvent> record) {
            String eventId = record.value().eventId();
            if (store.get(eventId) == null) {
                store.put(eventId, record.timestamp());
                context.forward(record);
            }
        }
    }

    // Construction du message DLQ - forme de l'enveloppe {"reason":..., "raw":...}
    // Jackson serialise un record via ses accesseurs : les noms de champs
    // produits dans le JSON sont donc exactement "reason" et "raw".
    private record DlqEnvelope(String reason, String raw) {
    }

    // Construction du message DLQ - raison precise du rejet
    // Reprend les memes verifications que isValid(), dans le meme ordre,
    // pour renvoyer la premiere raison qui explique le rejet (plutot qu'un
    // simple booleen). Necessaire pour respecter le critere "DLQ motivee :
    // on compte les raisons distinctes".
    private static String rejectReason(String rawJson) {
        PlaybackEvent event = JsonSerdes.parseOrNull(rawJson, PlaybackEvent.class);
        if (event == null) {
            return "JSON illisible (tronque, vide ou mal forme)";
        }
        if (!hasRequiredFields(event)) {
            return "champ requis absent ou null";
        }
        if (!hasValidTypesAndBounds(event)) {
            return "type invalide ou valeur hors bornes (position_seconds/timestamp)";
        }
        if (!hasValidEnums(event)) {
            return "valeur enum inconnue (event_type/device/region)";
        }
        return "raison indeterminee";
    }

    // Routage valide / invalide - regle de validite globale
    // Un message est valide si : il parse (pas un poison pill), ET les
    // champs requis sont presents, ET les types/bornes sont corrects, ET
    // les enums sont reconnus. Les 4 conditions viennent des taches
    // precedentes ; celle-ci ne fait que les combiner.
    private static boolean isValid(String rawJson) {
        PlaybackEvent event = JsonSerdes.parseOrNull(rawJson, PlaybackEvent.class);
        return event != null
                && hasRequiredFields(event)
                && hasValidTypesAndBounds(event)
                && hasValidEnums(event);
    }

    // Validation des champs requis
    // Un evenement dont un champ obligatoire est absent (null) est invalide.
    // Ne verifie que la presence des champs pour l'instant (types/bornes et
    // enums sont geres dans des taches suivantes).
    private static boolean hasRequiredFields(PlaybackEvent event) {
        return event.eventId() != null
                && event.eventType() != null
                && event.userId() != null
                && event.contentId() != null
                && event.timestamp() != null;
    }

    // Validation des types et des bornes
    // position_seconds doit etre >= 0 (une position de lecture negative n'a
    // pas de sens) et le timestamp doit etre une date ISO-8601 valide. Les
    // mauvais types bruts (ex. position_seconds: "douze") sont deja rejetes
    // en amont par parseOrNull (tache "parsing sur").
    private static boolean hasValidTypesAndBounds(PlaybackEvent event) {
        if (event.positionSeconds() == null || event.positionSeconds() < 0) {
            return false;
        }
        try {
            Instant.parse(event.timestamp());
        } catch (DateTimeParseException | NullPointerException e) {
            return false;
        }
        return true;
    }

    // Validation des enums
    // event_type/device/region doivent appartenir aux valeurs autorisees par
    // le README. Une valeur inconnue (faute de frappe du SDK, ex. "PLAYY")
    // finirait en branche morte plus loin si on ne la rejette pas ici.
    // Set.contains(null) renvoie false, donc un champ absent est aussi rejete.
    private static boolean hasValidEnums(PlaybackEvent event) {
        return VALID_EVENT_TYPES.contains(event.eventType())
                && VALID_DEVICES.contains(event.device())
                && VALID_REGIONS.contains(event.region());
    }
}
