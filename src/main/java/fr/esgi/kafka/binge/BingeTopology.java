package fr.esgi.kafka.binge;

import fr.esgi.kafka.binge.common.JsonSerdes;
import fr.esgi.kafka.binge.model.PlaybackEvent;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

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

    private BingeTopology() {
    }

    public static void build(StreamsBuilder builder) {

        // Flux brut : la valeur est du JSON... quand tout va bien.
        KStream<String, String> raw = builder.stream(
                Topics.PLAYBACK_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        raw.peek((key, value) -> System.out.println("[binge] " + key + " -> " + value));

        // Parsing sûr (poison pill)
        // JSON illisible (tronque, vide, non-JSON) -> null au lieu d'une
        // exception qui ferait planter l'appli en boucle.
        KStream<String, PlaybackEvent> parsed = raw.mapValues(
                value -> JsonSerdes.parseOrNull(value, PlaybackEvent.class));

        // -----------------------------------------------------------------
        // BINGE-1 - Ingestion fiable
        //   (validation branchee au fil des taches suivantes)
        //   Parser chaque message (model.PlaybackEvent), le valider, puis
        //   router : valides -> suite du pipeline, invalides -> Topics.DLQ
        //   (conserver le message original et ajouter la raison du rejet).
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // BINGE-2 - Compteur de vues par contenu        -> Topics.VIEWS_BY_TITLE
        // BINGE-3 - Top tendances par genre (fenetres + jointure catalogue)
        //                                               -> Topics.TRENDING
        // BINGE-4 - Alerte qualite "buffering storm"    -> Topics.ALERTS_QOE
        // BINGE-5 - Sessions utilisateur (session windows) -> Topics.SESSIONS
        // BINGE-6 (bonus) - API REST Interactive Queries (cf. README)
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
