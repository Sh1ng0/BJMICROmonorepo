package com.bjpractice.game_core.model;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

@NoArgsConstructor(force = true)
@AllArgsConstructor
@EqualsAndHashCode
public class Card {


    public enum Suit {
        HEARTS, DIAMONDS, CLUBS, SPADES
    }

    public enum Rank {
        TWO(2), THREE(3), FOUR(4), FIVE(5), SIX(6),
        SEVEN(7), EIGHT(8), NINE(9), TEN(10),
        JACK(10), QUEEN(10), KING(10), ACE(11);

        private final int value;

        Rank(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    @Getter private final Suit suit;
    @Getter private final Rank rank;



    @JsonIgnore
    public int getValue() {
        return rank.getValue();
    }

    @JsonIgnore
    public boolean isAce() {
        return rank == Rank.ACE;
    }

    @Override
    public String toString() {
        return rank + " of " + suit;
    }
}