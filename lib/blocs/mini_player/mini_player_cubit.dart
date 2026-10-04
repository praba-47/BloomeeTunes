// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:async';
import 'package:audio_service/audio_service.dart';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:Bloomee/blocs/media_player/bloomee_player_cubit.dart';
import 'package:Bloomee/core/models/exported.dart' hide MediaItem;
import 'package:Bloomee/core/adapters/track_adapter.dart';
import 'package:Bloomee/services/player/player_engine.dart';
import 'package:rxdart/rxdart.dart';

// ─── State ───────────────────────────────────────────────────────────────────

/// Visibility & playback state for the mini-player.
///
/// Intentionally simple — only 2 concrete states instead of 7.
/// The widget reads [isVisible], [isPlaying], and [isLoading] to decide layout.
class MiniPlayerState extends Equatable {
  /// The current track metadata. null → nothing loaded yet.
  final Track? track;

  /// Whether audio is actively playing.
  final bool isPlaying;

  /// Whether the engine is loading or buffering (show spinner).
  final bool isLoading;

  /// Whether the player is resolving the media URL (before engine loads).
  final bool isResolving;

  /// Whether the track has finished (show replay icon).
  final bool isCompleted;

  /// Whether the engine is in an error state.
  final bool hasError;

  const MiniPlayerState({
    this.track,
    this.isPlaying = false,
    this.isLoading = false,
    this.isResolving = false,
    this.isCompleted = false,
    this.hasError = false,
  });

  /// Mini player is visible whenever there's a track to show.
  bool get isVisible => track != null && track!.id != 'Null';

  const MiniPlayerState.hidden()
      : track = null,
        isPlaying = false,
        isLoading = false,
        isResolving = false,
        isCompleted = false,
        hasError = false;

  MiniPlayerState copyWith({
    Track? track,
    bool? isPlaying,
    bool? isLoading,
    bool? isResolving,
    bool? isCompleted,
    bool? hasError,
  }) {
    return MiniPlayerState(
      track: track ?? this.track,
      isPlaying: isPlaying ?? this.isPlaying,
      isLoading: isLoading ?? this.isLoading,
      isResolving: isResolving ?? this.isResolving,
      isCompleted: isCompleted ?? this.isCompleted,
      hasError: hasError ?? this.hasError,
    );
  }

  @override
  List<Object?> get props =>
      [track, isPlaying, isLoading, isResolving, isCompleted, hasError];
}

// ─── Cubit ───────────────────────────────────────────────────────────────────

/// Drives the mini-player widget.
class MiniPlayerCubit extends Cubit<MiniPlayerState> {
  final BloomeePlayerCubit _playerCubit;
  StreamSubscription? _sub;
  String? _dismissedId;
  bool _awaitingPause = false;

  MiniPlayerCubit({required BloomeePlayerCubit playerCubit})
      : _playerCubit = playerCubit,
        super(const MiniPlayerState.hidden()) {
    _listen();
  }

  /// User closed the mini player (swipe down / X button).
  void dismiss() {
    _dismissedId = state.track?.id;
    _awaitingPause = true;
    _playerCubit.bloomeePlayer.pause();
    emit(const MiniPlayerState.hidden());
  }

  void _listen() {
    _sub = Rx.combineLatest4<MediaItem?, EngineState, bool, bool,
        (MediaItem?, EngineState, bool, bool)>(
      _playerCubit.bloomeePlayer.mediaItem,
      Rx.defer(() => _playerCubit.bloomeePlayer.engine.stateStream,
          reusable: true),
      Rx.defer(() => _playerCubit.bloomeePlayer.engine.playingStream,
          reusable: true),
      _playerCubit.bloomeePlayer.isResolving,
      (media, engineState, playing, resolving) =>
          (media, engineState, playing, resolving),
    ).listen((record) {
      final (media, engineState, playing, resolving) = record;

      if (media == null || media.id == 'Null') {
        if (state.isVisible) emit(const MiniPlayerState.hidden());
        return;
      }

      if (_dismissedId != null) {
        if (media.id != _dismissedId) {
          _dismissedId = null; // new song -> show again
          _awaitingPause = false;
        } else if (!playing) {
          _awaitingPause = false;
          if (state.isVisible) emit(const MiniPlayerState.hidden());
          return;
        } else if (_awaitingPause) {
          return; // pause not applied yet
        } else {
          _dismissedId = null; // user resumed same song
        }
      }

      final track = mediaItemToTrack(media);

      emit(MiniPlayerState(
        track: track,
        isPlaying: playing,
        isLoading: engineState == EngineState.loading ||
            engineState == EngineState.buffering,
        isResolving: resolving,
        isCompleted: engineState == EngineState.completed,
        hasError: engineState == EngineState.error,
      ));
    });
  }

  @override
  Future<void> close() {
    _sub?.cancel();
    return super.close();
  }
}
