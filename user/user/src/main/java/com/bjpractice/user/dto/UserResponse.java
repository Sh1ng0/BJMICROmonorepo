package com.bjpractice.user.dto;

import java.math.BigDecimal;

public record UserResponse(
        Long id,
        String username,
        String email,
        BigDecimal balance,
        String role
) {}
