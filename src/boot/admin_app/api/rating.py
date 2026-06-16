from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER

from boot.admin_app.deps import depends, get_auth_user, require_role
from domain.entities.enums.user_role import UserRole
from domain.ports.access_manager import AuthUser
from domain.ports.repositories.rating_matches import RatingMatchesRepo, UserRatingsRepo
from domain.ports.repositories.users import UsersRepo
from domain.ports.repositories.weapon_types import WeaponTypesRepo
from use_cases.rating.create_rating_match import CreateRatingMatchInput, CreateRatingMatchUseCase
from use_cases.rating.get_leaderboard import GetLeaderboardUseCase
from use_cases.rating.record_pass import RecordPassInput, RecordPassUseCase
from use_cases.weapon_type.create_weapon_type import CreateWeaponTypeInput, CreateWeaponTypeUseCase
from use_cases.weapon_type.list_weapon_types import ListWeaponTypesUseCase

router = APIRouter(tags=["rating"])
_templates: Jinja2Templates | None = None


def set_templates(t: Jinja2Templates) -> None:
    global _templates
    _templates = t


def templates() -> Jinja2Templates:
    if _templates is None:
        raise RuntimeError("Templates not configured — call set_templates() during app setup")
    return _templates


@router.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard_redirect(request: Request, auth_user: AuthUser = Depends(get_auth_user)):
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/admin/rating", status_code=301)


@router.get("/rating", response_class=HTMLResponse)
async def rating_page(
    request: Request,
    auth_user: AuthUser = Depends(get_auth_user),
    users_repo: UsersRepo = Depends(depends(UsersRepo)),
    ratings_repo: UserRatingsRepo = Depends(depends(UserRatingsRepo)),
):
    select_users = await users_repo.list_for_select()
    users_by_id = {u.id: u.name for u in select_users}

    rows_with_wins = await ratings_repo.list_all_with_wins()

    leaderboard = []
    for rating, wins in rows_with_wins:
        mp = rating.matches_played
        win_rate = round(wins / mp * 100) if mp > 0 else 0
        leaderboard.append({
            "user_id": rating.user_id,
            "name": users_by_id.get(rating.user_id, "—"),
            "elo": round(rating.elo),
            "matches": mp,
            "wins": wins,
            "win_rate": win_rate,
            "calibrating": mp < 10,
        })

    return templates().TemplateResponse(
        request,
        "rating/leaderboard.html",
        {"leaderboard": leaderboard, "auth_user": auth_user},
    )


@router.get("/rating/new", response_class=HTMLResponse)
async def rating_new_match_page(
    request: Request,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    list_weapons: ListWeaponTypesUseCase = Depends(depends(ListWeaponTypesUseCase)),
    users_repo: UsersRepo = Depends(depends(UsersRepo)),
):
    weapons = await list_weapons()
    select_users = await users_repo.list_for_select()
    return templates().TemplateResponse(
        request,
        "rating/new_match.html",
        {"weapons": weapons, "users": select_users, "auth_user": auth_user},
    )


@router.post("/rating/match")
async def create_rating_match(
    request: Request,
    left_user_id: UUID = Form(...),
    right_user_id: UUID = Form(...),
    weapon_type_ids: list[UUID] = Form(...),
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: CreateRatingMatchUseCase = Depends(depends(CreateRatingMatchUseCase)),
):
    match = await use_case(
        auth_user,
        CreateRatingMatchInput(
            left_user_id=left_user_id,
            right_user_id=right_user_id,
            weapon_type_ids=weapon_type_ids,
        ),
    )
    return RedirectResponse(url=f"/admin/rating/match/{match.id}", status_code=HTTP_303_SEE_OTHER)


@router.get("/rating/match/{match_id}", response_class=HTMLResponse)
async def rating_match_page(
    request: Request,
    match_id: UUID,
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    matches_repo: RatingMatchesRepo = Depends(depends(RatingMatchesRepo)),
    users_repo: UsersRepo = Depends(depends(UsersRepo)),
    weapons_repo: WeaponTypesRepo = Depends(depends(WeaponTypesRepo)),
    ratings_repo: UserRatingsRepo = Depends(depends(UserRatingsRepo)),
):
    match = await matches_repo.get_match_by_id(match_id)
    left_user = await users_repo.get_by_id(match.left_user_id)
    right_user = await users_repo.get_by_id(match.right_user_id)

    # Build weapon name lookup for the 3 bout slots
    all_weapons = await weapons_repo.list_all()
    weapons_by_id = {w.id: w.name for w in all_weapons}
    bout_weapons = [weapons_by_id.get(wid, "?") for wid in match.weapon_type_ids]

    # Current ELO for result screen (post-match)
    left_rating = await ratings_repo.get_by_user_id(match.left_user_id)
    right_rating = await ratings_repo.get_by_user_id(match.right_user_id)

    return templates().TemplateResponse(
        request,
        "rating/match.html",
        {
            "match": match,
            "left_user": left_user,
            "right_user": right_user,
            "bout_weapons": bout_weapons,
            "left_rating": left_rating,
            "right_rating": right_rating,
            "auth_user": auth_user,
        },
    )


@router.post("/rating/match/{match_id}/pass")
async def record_pass(
    match_id: UUID,
    winner_user_id: UUID = Form(...),
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: RecordPassUseCase = Depends(depends(RecordPassUseCase)),
):
    await use_case(auth_user, match_id, RecordPassInput(winner_user_id=winner_user_id))
    return RedirectResponse(url=f"/admin/rating/match/{match_id}", status_code=HTTP_303_SEE_OTHER)


@router.post("/rating/weapons")
async def create_weapon(
    name: str = Form(...),
    auth_user: AuthUser = Depends(require_role(UserRole.INSTRUCTOR)),
    use_case: CreateWeaponTypeUseCase = Depends(depends(CreateWeaponTypeUseCase)),
):
    await use_case(auth_user, CreateWeaponTypeInput(name=name))
    return RedirectResponse(url="/admin/rating", status_code=HTTP_303_SEE_OTHER)
