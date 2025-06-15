package com.bjpractice.game_core.kafka.event;


import com.bjpractice.game_core.model.Game;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.UUID;

import java.math.BigDecimal;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GameFinishedEvent {

    private UUID gameId;
    private UUID betId;   // <-- AÑADIDO Y ESENCIAL
    private Long userId;
    private Game.GameResult result;
    private boolean playerHasBlackjack;

    // El campo 'payoutMultiplier' se ha eliminado por completo.
}
