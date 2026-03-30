"use client";

import { useState, lazy, Suspense } from "react";
import { SmilePlus } from "lucide-react";
import { Popover, PopoverTrigger, PopoverContent } from "@/components/ui/popover";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { useActorName } from "@/features/workspace";

const EmojiPicker = lazy(() =>
  import("@/components/common/emoji-picker").then((m) => ({ default: m.EmojiPicker })),
);

const QUICK_EMOJIS = ["👍", "👎", "❤️", "😄", "🎉", "😕", "🚀", "👀"];

interface ReactionItem {
  id: string;
  actor_type: string;
  actor_id: string;
  emoji: string;
}

interface GroupedReaction {
  emoji: string;
  count: number;
  reacted: boolean;
  actors: { type: string; id: string }[];
}

function groupReactions(reactions: ReactionItem[], currentUserId?: string): GroupedReaction[] {
  const map = new Map<string, GroupedReaction>();
  for (const r of reactions) {
    let group = map.get(r.emoji);
    if (!group) {
      group = { emoji: r.emoji, count: 0, reacted: false, actors: [] };
      map.set(r.emoji, group);
    }
    group.count++;
    group.actors.push({ type: r.actor_type, id: r.actor_id });
    if (r.actor_type === "member" && r.actor_id === currentUserId) {
      group.reacted = true;
    }
  }
  return Array.from(map.values());
}

export function ReactionBar({
  reactions,
  currentUserId,
  onToggle,
  className,
}: {
  reactions: ReactionItem[];
  currentUserId?: string;
  onToggle: (emoji: string) => void;
  className?: string;
}) {
  const [pickerOpen, setPickerOpen] = useState(false);
  const [showFullPicker, setShowFullPicker] = useState(false);
  const grouped = groupReactions(reactions, currentUserId);
  const { getActorName } = useActorName();

  const handlePickerOpenChange = (open: boolean) => {
    setPickerOpen(open);
    if (!open) setShowFullPicker(false);
  };

  return (
    <div className={`flex flex-wrap items-center gap-1.5 ${className ?? ""}`}>
      {grouped.map((g) => (
        <Tooltip key={g.emoji}>
          <TooltipTrigger
            render={
              <button
                type="button"
                onClick={() => onToggle(g.emoji)}
                className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs transition-colors hover:bg-accent ${
                  g.reacted
                    ? "border-primary/40 bg-primary/10 text-primary"
                    : "border-border text-muted-foreground"
                }`}
              >
                <span>{g.emoji}</span>
                <span>{g.count}</span>
              </button>
            }
          />
          <TooltipContent side="top">
            {g.actors.map((a) => getActorName(a.type, a.id)).join(", ")}
          </TooltipContent>
        </Tooltip>
      ))}
      <Popover open={pickerOpen} onOpenChange={handlePickerOpenChange}>
        <PopoverTrigger
          render={
            <button
              type="button"
              className="inline-flex items-center justify-center h-6 w-6 rounded-full text-muted-foreground hover:bg-accent hover:text-foreground transition-colors"
            >
              <SmilePlus className="h-3.5 w-3.5" />
            </button>
          }
        />
        <PopoverContent align="start" className="w-auto p-0">
          {showFullPicker ? (
            <Suspense fallback={<div className="p-4 text-sm text-muted-foreground">Loading...</div>}>
              <EmojiPicker
                onSelect={(emoji) => {
                  onToggle(emoji);
                  setPickerOpen(false);
                  setShowFullPicker(false);
                }}
              />
            </Suspense>
          ) : (
            <div className="p-2">
              <div className="flex gap-1">
                {QUICK_EMOJIS.map((emoji) => (
                  <button
                    key={emoji}
                    type="button"
                    onClick={() => {
                      onToggle(emoji);
                      setPickerOpen(false);
                    }}
                    className="h-8 w-8 flex items-center justify-center rounded hover:bg-accent text-base transition-colors"
                  >
                    {emoji}
                  </button>
                ))}
              </div>
              <button
                type="button"
                onClick={() => setShowFullPicker(true)}
                className="mt-1.5 w-full text-xs text-muted-foreground hover:text-foreground text-center py-1 rounded hover:bg-accent transition-colors"
              >
                More emojis...
              </button>
            </div>
          )}
        </PopoverContent>
      </Popover>
    </div>
  );
}
