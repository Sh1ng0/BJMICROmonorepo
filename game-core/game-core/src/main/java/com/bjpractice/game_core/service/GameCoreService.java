package com.bjpractice.game_core.service;


import com.bjpractice.game_core.dto.GameDTO;
import com.bjpractice.game_core.exception.BetAlreadyInGameException;
import com.bjpractice.game_core.kafka.event.GameFinishedEvent;
import com.bjpractice.game_core.kafka.producer.GameEventProducer;
import com.bjpractice.game_core.mapper.GameMapper;
import com.bjpractice.game_core.model.Game;
import com.bjpractice.game_core.model.GameEntity;
import com.bjpractice.game_core.repository.GameRepository;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class GameCoreService {

    private final GameRepository gameRepository;

    private final GameEventProducer gameEventProducer;

    private final GameMapper gameMapper;

    public GameCoreService(GameRepository gameRepository, GameEventProducer gameEventProducer, GameMapper gameMapper) {
        this.gameRepository = gameRepository;
        this.gameEventProducer = gameEventProducer;
        this.gameMapper = gameMapper;
    }


    public GameDTO startGame(Long userId, UUID betId) {


        if (gameRepository.findByBetId(betId).isPresent()) {
            throw new BetAlreadyInGameException("Ya existe una partida asociada al betId: " + betId);
        }


        UUID gameId = UUID.randomUUID();
        GameEntity gameEntity = new GameEntity(gameId, userId, betId);


        Game game = gameEntity.getGameLogic();
        game.startGame();


        gameRepository.save(gameEntity);


        if (game.isGameOver()) {
            GameFinishedEvent event = new GameFinishedEvent(
                    gameEntity.getId(),
                    gameEntity.getBetId(),
                    gameEntity.getUserId(),
                    game.getResult(),
                    game.getPlayer().hasBlackjack()
            );
            gameEventProducer.sendGameFinishedEvent(event);
        }


        return gameMapper.toDTO(gameEntity);
    }


}
