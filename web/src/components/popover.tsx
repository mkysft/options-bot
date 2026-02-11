"use client";

import * as React from "react";
import { Popover as PopoverPrimitive } from "@base-ui/react/popover";

import { cn } from "@/lib/utils";

function Popover(props: PopoverPrimitive.Root.Props) {
  return <PopoverPrimitive.Root data-slot="popover" {...props} />;
}

function PopoverTrigger({
  className,
  ...props
}: PopoverPrimitive.Trigger.Props) {
  return (
    <PopoverPrimitive.Trigger
      className={cn(className)}
      data-slot="popover-trigger"
      {...props}
    />
  );
}

interface PopoverContentProps extends PopoverPrimitive.Popup.Props {
  align?: PopoverPrimitive.Positioner.Props["align"];
  alignOffset?: PopoverPrimitive.Positioner.Props["alignOffset"];
  side?: PopoverPrimitive.Positioner.Props["side"];
  sideOffset?: PopoverPrimitive.Positioner.Props["sideOffset"];
  collisionPadding?: PopoverPrimitive.Positioner.Props["collisionPadding"];
  portal?: boolean;
}

const PopoverContent = React.forwardRef<HTMLDivElement, PopoverContentProps>(
  (
    {
      className,
      align = "center",
      alignOffset,
      side = "bottom",
      sideOffset = 8,
      collisionPadding = 8,
      portal = true,
      ...props
    },
    ref,
  ) => {
    const content = (
      <PopoverPrimitive.Positioner
        align={align}
        alignOffset={alignOffset}
        collisionPadding={collisionPadding}
        className="z-[120]"
        positionMethod="fixed"
        side={side}
        sideOffset={sideOffset}
      >
        <PopoverPrimitive.Popup
          ref={ref}
          className={cn(
            "z-[120] w-72 rounded-lg border bg-popover p-3 text-popover-foreground shadow-lg/10 outline-none not-dark:bg-clip-padding",
            "transition-[opacity,transform] duration-150 ease-out data-starting-style:opacity-0 data-starting-style:scale-[0.98] data-ending-style:opacity-0 data-ending-style:scale-[0.98]",
            className,
          )}
          data-slot="popover-content"
          {...props}
        />
      </PopoverPrimitive.Positioner>
    );

    if (!portal) return content;
    return <PopoverPrimitive.Portal>{content}</PopoverPrimitive.Portal>;
  },
);

PopoverContent.displayName = "PopoverContent";

export { Popover, PopoverTrigger, PopoverContent };
