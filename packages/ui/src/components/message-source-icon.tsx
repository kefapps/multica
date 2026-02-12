"use client";

import { Monitor, Smartphone, Send } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@multica/ui/components/ui/tooltip";
import type { MessageSource } from "@multica/store";

interface MessageSourceIconProps {
  source: MessageSource;
  className?: string;
}

/** Get icon component for message source */
function getSourceIcon(source: MessageSource) {
  switch (source.type) {
    case "local":
      return Monitor;
    case "gateway":
      return Smartphone;
    case "channel":
      return Send;
    default:
      return Monitor;
  }
}

/** Get tooltip text for message source */
function getSourceTooltip(source: MessageSource): string {
  switch (source.type) {
    case "local":
      return "Local";
    case "gateway":
      return `Remote: ${source.deviceId}`;
    case "channel":
      return `${source.channelId}: ${source.conversationId}`;
    default:
      return "Unknown";
  }
}

export function MessageSourceIcon({ source, className }: MessageSourceIconProps) {
  const Icon = getSourceIcon(source);
  const tooltip = getSourceTooltip(source);

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger className="inline-flex">
          <Icon className={className ?? "w-3 h-3 text-muted-foreground"} />
        </TooltipTrigger>
        <TooltipContent side="top" className="text-xs">
          {tooltip}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
