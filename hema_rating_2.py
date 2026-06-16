"""
Улучшенная система рейтинга HEMA на основе Glicko-2
Использует систему "очков убедительности" для более точного расчёта
"""

import math
from models import Match, MatchRating, FighterMatchRating


class ImprovedMatchRatingCalculator:
    """
    Улучшенный калькулятор рейтинга с системой очков убедительности.
    
    Система очков:
    - Каждый выигранный сход: +0.1 очка
    - Каждый выигранный бой: +0.1 очка
    - Победа в матче: +0.1 очка
    
    Максимум: 1.2 (победа 3-0 со счётом 3:0, 3:0, 3:0)
    Минимум: 0.0 (поражение 0-3 со счётом 0:3, 0:3, 0:3)
    
    Разница очков убедительности определяет силу изменения рейтинга.
    """
    
    # Константы для системы очков
    # Ваша система:
    # - Каждый сход: 0.1 (макс 9 × 0.1 = 0.9)
    # - Каждый бой: 0.1 (макс 3 × 0.1 = 0.3)
    # - Победа в матче: 0.1
    # Максимум: 0.9 + 0.3 + 0.1 = 1.3 ✅
    POINTS_PER_EXCHANGE = 0.1  # Очки за каждый выигранный сход
    POINTS_PER_FIGHT = 0.1     # Очки за каждый выигранный бой
    POINTS_PER_MATCH = 0.1     # Очки за победу в матче
    MAX_CONVINCING_SCORE = 1.3  # Максимальные очки убедительности
    
    # Параметры системы (адаптация Glicko)
    MIN_RD = 50.0   # Минимальный RD для опытных бойцов
    MAX_RD = 350.0  # Максимальный RD для новичков
    BASE_K = 30.0   # Базовый K-фактор (настроен для достижения целевых изменений)
    
    def _calculate_convincing_score(
        self, 
        exchanges_won: int, 
        fights_won: int,
        match_won: bool
    ) -> float:
        """
        Рассчитывает очки убедительности для бойца.
        
        Args:
            exchanges_won: Количество выигранных сходов (0-9)
            fights_won: Количество выигранных боёв (0-3)
            match_won: Победа в матче (True/False)
        
        Returns:
            Очки убедительности от 0.0 до 1.3
            - Максимум (победа 3-0 в сухую): 9 сходов + 3 боя + матч = 1.3
            - Минимум (поражение 0-3 в сухую): 0
        """
        score = 0.0
        
        # Очки за сходы (макс 0.9)
        score += exchanges_won * self.POINTS_PER_EXCHANGE
        
        # Очки за бои (макс 0.3)
        score += fights_won * self.POINTS_PER_FIGHT
        
        # Очки за победу в матче (0.1)
        if match_won:
            score += self.POINTS_PER_MATCH
        
        return score
    
    def _calculate_rd(self, past_matches: int) -> float:
        """
        Вычисляет Rating Deviation (RD) на основе опыта бойца.
        
        RD определяет неопределённость рейтинга (из системы Glicko):
        - Новички (0 матчей): RD = 350 (высокая неопределённость)
        - Опытные (10+ матчей): RD → 50 (низкая неопределённость)
        
        Формула: экспоненциальное убывание
        """
        rd = self.MAX_RD * math.exp(-past_matches / 10.0) + self.MIN_RD
        return min(self.MAX_RD, max(self.MIN_RD, rd))
    
    def _calculate_expected_convincing_score(self, rating_diff: float) -> float:
        """
        Рассчитывает ожидаемые очки убедительности на основе разницы рейтингов.
        
        Использует логистическую функцию из системы Glicko.
        
        Args:
            rating_diff: Разница в рейтинге (мой - оппонента)
        
        Returns:
            Ожидаемые очки убедительности от 0.0 до 1.3
        """
        # Базовая вероятность победы по Glicko
        win_probability = 1.0 / (1.0 + math.pow(10, -rating_diff / 400.0))
        
        # Преобразуем вероятность в ожидаемые очки убедительности
        # 0.5 (50%) → 0.65 (небольшой перевес)
        # 1.0 (100%) → 1.3 (победа в сухую)
        # 0.0 (0%) → 0.0 (поражение в сухую)
        expected_score = win_probability * self.MAX_CONVINCING_SCORE
        
        return expected_score
    
    def _calculate_rating_change(
        self,
        actual_convincing_score: float,
        expected_convincing_score: float,
        rd: float,
        rating_diff: float
    ) -> float:
        """
        Рассчитывает изменение рейтинга по формуле Glicko.
        
        Формула: rating_change = K * (actual_normalized - expected_normalized)
        
        Где:
        - K = BASE_K * (RD / 100) — зависит от неопределённости рейтинга
        - actual/expected нормализованы к диапазону [0, 1] для стабильности
        
        Args:
            actual_convincing_score: Фактические очки убедительности (0.0-1.3)
            expected_convincing_score: Ожидаемые очки убедительности (0.0-1.3)
            rd: Rating Deviation (неопределённость рейтинга)
            rating_diff: Разница в рейтинге (мой - оппонента)
        
        Returns:
            Изменение рейтинга (естественно ограниченное через RD)
        """
        # Нормализуем очки к диапазону [0, 1] для стабильности формулы
        actual_normalized = actual_convincing_score / self.MAX_CONVINCING_SCORE
        expected_normalized = expected_convincing_score / self.MAX_CONVINCING_SCORE
        
        # Разница между фактом и ожиданием (теперь в диапазоне [-1, 1])
        score_diff = actual_normalized - expected_normalized
        
        # K-фактор зависит от RD (формула Glicko)
        # Новички (RD=350): K = 22 * 3.5 = 77
        # Опытные (RD=50): K = 22 * 0.5 = 11
        k_factor = self.BASE_K * (rd / 100.0)
        
        # Итоговое изменение (без upset_multiplier — RD уже учитывает опыт)
        rating_change = k_factor * score_diff
        
        return rating_change
    
    def calculate_rating(self, match: Match) -> MatchRating:
        """
        Рассчитывает новые рейтинги для обоих бойцов после матча.
        
        Использует систему очков убедительности для определения
        силы изменения рейтинга.
        """
        # Parse ratings
        r1 = float(match.fighter_1.rating)
        r2 = float(match.fighter_2.rating)
        
        # Разница в рейтинге
        rating_diff_f1 = r1 - r2  # Положительная, если f1 сильнее
        rating_diff_f2 = r2 - r1  # Положительная, если f2 сильнее
        
        # Рассчитываем RD (Rating Deviation) для каждого бойца
        rd1 = self._calculate_rd(match.fighter_1.past_matches_count)
        rd2 = self._calculate_rd(match.fighter_2.past_matches_count)
        
        # Collect scores
        fights = [match.fight_1, match.fight_2, match.fight_3]
        l_fight_scores = [f.l_score for f in fights]
        r_fight_scores = [f.r_score for f in fights]
        
        # Подсчёт сходов
        total_l = sum(l_fight_scores)
        total_r = sum(r_fight_scores)
        
        if total_l + total_r != 9:
            raise ValueError("Total exchanges across all fights must be exactly 9")
        
        # Подсчёт боёв
        fights_won_l = sum(1 for score in l_fight_scores if score > (3 - score))
        fights_won_r = sum(1 for score in r_fight_scores if score > (3 - score))
        
        # Определяем победителя матча ПО БОЯМ (не по сходам!)
        is_left_winner = fights_won_l > fights_won_r
        is_right_winner = fights_won_r > fights_won_l
        
        # Рассчитываем очки убедительности
        convincing_l = self._calculate_convincing_score(
            exchanges_won=total_l,
            fights_won=fights_won_l,
            match_won=is_left_winner
        )
        
        convincing_r = self._calculate_convincing_score(
            exchanges_won=total_r,
            fights_won=fights_won_r,
            match_won=is_right_winner
        )
        
        # Рассчитываем ожидаемые очки убедительности на основе разницы рейтингов
        expected_convincing_l = self._calculate_expected_convincing_score(rating_diff_f1)
        expected_convincing_r = self._calculate_expected_convincing_score(rating_diff_f2)
        
        # Рассчитываем изменения рейтинга по формуле Glicko
        rating_change_1 = self._calculate_rating_change(
            actual_convincing_score=convincing_l,
            expected_convincing_score=expected_convincing_l,
            rd=rd1,
            rating_diff=rating_diff_f1
        )
        
        rating_change_2 = self._calculate_rating_change(
            actual_convincing_score=convincing_r,
            expected_convincing_score=expected_convincing_r,
            rd=rd2,
            rating_diff=rating_diff_f2
        )
        
        # Применяем изменения
        new_rating_1 = r1 + rating_change_1
        new_rating_2 = r2 + rating_change_2
        
        # Правило: победитель не может потерять очки
        if is_left_winner and rating_change_1 < 0:
            new_rating_1 = r1
            rating_change_1 = 0.0
        
        if is_right_winner and rating_change_2 < 0:
            new_rating_2 = r2
            rating_change_2 = 0.0
        
        # Обновляем RD (упрощённо - просто уменьшаем после матча)
        # В реальном Glicko-2 это сложнее, но для простоты:
        post_rd1 = max(self.MIN_RD, rd1 * 0.95)
        post_rd2 = max(self.MIN_RD, rd2 * 0.95)
        
        # Рассчитываем ожидаемый результат (для статистики)
        # Используем простую формулу на основе разницы рейтингов
        expected_l = 1.0 / (1.0 + math.pow(10, -rating_diff_f1 / 400.0))
        expected_r = 1.0 / (1.0 + math.pow(10, -rating_diff_f2 / 400.0))
        
        # Actual score (нормализованный к 0-1)
        actual_l = convincing_l / 1.3  # Максимум 1.3, нормализуем к 1.0
        actual_r = convincing_r / 1.3
        
        # Формируем результаты
        left = FighterMatchRating(
            name=match.fighter_1.name,
            old_rating=r1,
            new_rating=new_rating_1,
            rating_change=rating_change_1,
            pre_rd=round(rd1, 1),
            post_rd=round(post_rd1, 1),
            actual_score=round(actual_l, 3),
            expected_score=round(expected_l, 3),
            total_exchanges_won=total_l,
            fight_scores=l_fight_scores,
            is_match_winner=is_left_winner,
            rating_difference_pre=round(rating_diff_f1, 1),
            convincing_victory_factor=round(convincing_l, 3),
        )
        
        right = FighterMatchRating(
            name=match.fighter_2.name,
            old_rating=r2,
            new_rating=new_rating_2,
            rating_change=rating_change_2,
            pre_rd=round(rd2, 1),
            post_rd=round(post_rd2, 1),
            actual_score=round(actual_r, 3),
            expected_score=round(expected_r, 3),
            total_exchanges_won=total_r,
            fight_scores=r_fight_scores,
            is_match_winner=is_right_winner,
            rating_difference_pre=round(rating_diff_f2, 1),
            convincing_victory_factor=round(convincing_r, 3),
        )
        
        return MatchRating(left_fighter=left, right_fighter=right)

